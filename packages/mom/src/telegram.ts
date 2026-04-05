import { hydrateFiles } from "@grammyjs/files";
import { appendFileSync, existsSync, mkdirSync } from "fs";
import { Bot, type Context, InputFile } from "grammy";
import { basename, join } from "path";
import * as log from "./log.js";
import type { Attachment, ChannelStore } from "./store.js";
import type { ChannelInfo, ChatBot, ChatEvent, MomHandler, UserInfo } from "./types.js";

// ============================================================================
// mrkdwn to HTML converter (Slack-style formatting → Telegram HTML)
// ============================================================================

function mrkdwnToHtml(text: string): string {
	// First, escape HTML entities in the raw text
	let html = text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

	// Code blocks: ```code``` → <pre>code</pre>
	html = html.replace(/```([\s\S]*?)```/g, (_m, code) => `<pre>${code}</pre>`);

	// Inline code: `code` → <code>code</code>
	html = html.replace(/`([^`]+)`/g, (_m, code) => `<code>${code}</code>`);

	// Bold: *text* → <b>text</b> (but not inside code blocks)
	// Only match *text* where text doesn't contain newlines
	html = html.replace(/(?<!\w)\*([^*\n]+)\*(?!\w)/g, "<b>$1</b>");

	// Italic: _text_ → <i>text</i>
	html = html.replace(/(?<!\w)_([^_\n]+)_(?!\w)/g, "<i>$1</i>");

	return html;
}

// ============================================================================
// Per-channel queue for sequential processing
// ============================================================================

type QueuedWork = () => Promise<void>;

class ChannelQueue {
	private queue: QueuedWork[] = [];
	private processing = false;

	enqueue(work: QueuedWork): void {
		this.queue.push(work);
		this.processNext();
	}

	size(): number {
		return this.queue.length;
	}

	private async processNext(): Promise<void> {
		if (this.processing || this.queue.length === 0) return;
		this.processing = true;
		const work = this.queue.shift()!;
		try {
			await work();
		} catch (err) {
			log.logWarning("Queue error", err instanceof Error ? err.message : String(err));
		}
		this.processing = false;
		this.processNext();
	}
}

// ============================================================================
// TelegramBot
// ============================================================================

export class TelegramBot implements ChatBot {
	private bot: Bot;
	private handler: MomHandler;
	private workingDir: string;
	private store: ChannelStore;
	private botUsername: string | null = null;

	private users = new Map<string, UserInfo>();
	private channels = new Map<string, ChannelInfo>();
	private queues = new Map<string, ChannelQueue>();
	private soloChannels = new Set<string>();

	constructor(handler: MomHandler, config: { token: string; workingDir: string; store: ChannelStore }) {
		this.handler = handler;
		this.workingDir = config.workingDir;
		this.store = config.store;
		this.bot = new Bot(config.token);
		this.bot.api.config.use(hydrateFiles(config.token));
	}

	// ==========================================================================
	// Public API
	// ==========================================================================

	async start(): Promise<void> {
		const me = await this.bot.api.getMe();
		this.botUsername = me.username || null;
		log.logInfo(`Telegram bot: @${this.botUsername} (${me.id})`);

		this.setupMessageHandler();

		// Start long polling (non-blocking)
		this.bot.start({
			onStart: () => {
				log.logConnected();
			},
		});
	}

	getUser(userId: string): UserInfo | undefined {
		return this.users.get(userId);
	}

	getChannel(channelId: string): ChannelInfo | undefined {
		return this.channels.get(channelId);
	}

	getAllUsers(): UserInfo[] {
		return Array.from(this.users.values());
	}

	getAllChannels(): ChannelInfo[] {
		return Array.from(this.channels.values());
	}

	async postMessage(channel: string, text: string): Promise<string> {
		const html = mrkdwnToHtml(text);
		try {
			const msg = await this.bot.api.sendMessage(Number(channel), html, { parse_mode: "HTML" });
			return msg.message_id.toString();
		} catch {
			// Fallback to plain text if HTML parsing fails
			const msg = await this.bot.api.sendMessage(Number(channel), text);
			return msg.message_id.toString();
		}
	}

	async updateMessage(channel: string, messageId: string, text: string): Promise<void> {
		const html = mrkdwnToHtml(text);
		try {
			await this.bot.api.editMessageText(Number(channel), Number(messageId), html, { parse_mode: "HTML" });
		} catch (err) {
			const errMsg = err instanceof Error ? err.message : String(err);
			if (errMsg.includes("message is not modified")) return;
			// Retry without HTML if parse failed
			try {
				await this.bot.api.editMessageText(Number(channel), Number(messageId), text);
			} catch (retryErr) {
				const retryMsg = retryErr instanceof Error ? retryErr.message : String(retryErr);
				if (!retryMsg.includes("message is not modified")) {
					throw retryErr;
				}
			}
		}
	}

	async deleteMessage(channel: string, messageId: string): Promise<void> {
		await this.bot.api.deleteMessage(Number(channel), Number(messageId));
	}

	async postInThread(channel: string, parentMessageId: string, text: string): Promise<string> {
		const html = mrkdwnToHtml(text);
		const opts = { reply_parameters: { message_id: Number(parentMessageId) } };
		try {
			const msg = await this.bot.api.sendMessage(Number(channel), html, { ...opts, parse_mode: "HTML" });
			return msg.message_id.toString();
		} catch {
			const msg = await this.bot.api.sendMessage(Number(channel), text, opts);
			return msg.message_id.toString();
		}
	}

	async uploadFile(channel: string, filePath: string, title?: string): Promise<void> {
		const fileName = title || basename(filePath);
		await this.bot.api.sendDocument(Number(channel), new InputFile(filePath), {
			caption: fileName,
		});
	}

	logToFile(channel: string, entry: object): void {
		const dir = join(this.workingDir, channel);
		if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
		appendFileSync(join(dir, "log.jsonl"), `${JSON.stringify(entry)}\n`);
	}

	logBotResponse(channel: string, text: string, messageId: string): void {
		this.logToFile(channel, {
			date: new Date().toISOString(),
			ts: messageId,
			user: "bot",
			text,
			attachments: [],
			isBot: true,
		});
	}

	enqueueEvent(event: ChatEvent): boolean {
		const queue = this.getQueue(event.channel);
		if (queue.size() >= 5) {
			log.logWarning(`Event queue full for ${event.channel}, discarding: ${event.text.substring(0, 50)}`);
			return false;
		}
		log.logInfo(`Enqueueing event for ${event.channel}: ${event.text.substring(0, 50)}`);
		queue.enqueue(() => this.handler.handleEvent(event, this, true));
		return true;
	}

	// ==========================================================================
	// Private
	// ==========================================================================

	private getQueue(channelId: string): ChannelQueue {
		let queue = this.queues.get(channelId);
		if (!queue) {
			queue = new ChannelQueue();
			this.queues.set(channelId, queue);
		}
		return queue;
	}

	private setupMessageHandler(): void {
		this.bot.on("message", async (ctx) => {
			const msg = ctx.message;
			if (!msg.text && !msg.document && !msg.photo && !msg.caption) return;

			const from = msg.from;
			if (!from || from.is_bot) return;

			const chatId = msg.chat.id.toString();
			const userId = from.id.toString();
			const messageId = msg.message_id.toString();

			// Track user
			const userName = from.username || `user${from.id}`;
			const displayName = [from.first_name, from.last_name].filter(Boolean).join(" ") || userName;
			if (!this.users.has(userId)) {
				this.users.set(userId, { id: userId, userName, displayName });
			}

			// Track chat/channel
			const chatName = msg.chat.type === "private" ? `DM:${userName}` : (msg.chat as any).title || chatId;
			if (!this.channels.has(chatId)) {
				this.channels.set(chatId, { id: chatId, name: chatName });
			}

			const isDM = msg.chat.type === "private";
			const isMentioned =
				this.botUsername !== null && (msg.text || msg.caption || "").includes(`@${this.botUsername}`);

			// Detect solo channels on first message
			if (!isDM && !this.soloChannels.has(chatId)) {
				try {
					const count = await ctx.api.getChatMemberCount(msg.chat.id);
					if (count <= 2) this.soloChannels.add(chatId);
				} catch {
					// Ignore — may not have permission
				}
			}
			const isSolo = this.soloChannels.has(chatId);

			// Text from message body or caption (for photos/documents with captions)
			let rawText = msg.text || msg.caption || "";

			// In group chats, only respond to @mentions (unless solo)
			if (!isDM && !isSolo && !isMentioned) {
				this.logUserMessage(chatId, userId, messageId, rawText, userName, displayName, []);
				return;
			}

			// Strip @mention from text
			if (this.botUsername) {
				rawText = rawText.replace(new RegExp(`@${this.botUsername}`, "gi"), "").trim();
			}

			// Download attachments (photos/documents)
			const attachments = await this.downloadAttachments(ctx, chatId);

			const chatEvent: ChatEvent = {
				type: isDM ? "dm" : "mention",
				channel: chatId,
				messageId,
				user: userId,
				text: rawText,
				attachments,
			};

			// Log user message
			this.logUserMessage(chatId, userId, messageId, rawText, userName, displayName, attachments);

			// Check for stop command
			if (chatEvent.text.toLowerCase().trim() === "stop") {
				if (this.handler.isRunning(chatId)) {
					this.handler.handleStop(chatId, this);
				} else {
					this.postMessage(chatId, "_Nothing running_");
				}
				return;
			}

			// Check if busy
			if (this.handler.isRunning(chatId)) {
				const stopHint = isDM ? "`stop`" : `\`@${this.botUsername || "mom"} stop\``;
				this.postMessage(chatId, `_Already working. Say ${stopHint} to cancel._`);
			} else {
				this.getQueue(chatId).enqueue(() => this.handler.handleEvent(chatEvent, this));
			}
		});
	}

	private async downloadAttachments(ctx: Context, chatId: string): Promise<Attachment[]> {
		const attachments: Attachment[] = [];
		const msg = ctx.message;
		if (!msg) return attachments;

		try {
			if (msg.photo && msg.photo.length > 0) {
				// Get the largest photo (last in array)
				const photo = msg.photo[msg.photo.length - 1];
				const file = await ctx.api.getFile(photo.file_id);
				const ext = file.file_path?.split(".").pop() || "jpg";
				const filename = `${Date.now()}_photo.${ext}`;
				const localRelPath = `${chatId}/attachments/${filename}`;
				const localAbsPath = join(this.workingDir, localRelPath);

				const dir = join(this.workingDir, chatId, "attachments");
				if (!existsSync(dir)) mkdirSync(dir, { recursive: true });

				await (file as any).download(localAbsPath);
				attachments.push({ original: `photo.${ext}`, local: localRelPath });
				log.logInfo(`Downloaded photo attachment: ${localRelPath}`);
			}

			if (msg.document) {
				const file = await ctx.api.getFile(msg.document.file_id);
				const originalName = msg.document.file_name || "document";
				const filename = `${Date.now()}_${originalName.replace(/[^a-zA-Z0-9._-]/g, "_")}`;
				const localRelPath = `${chatId}/attachments/${filename}`;
				const localAbsPath = join(this.workingDir, localRelPath);

				const dir = join(this.workingDir, chatId, "attachments");
				if (!existsSync(dir)) mkdirSync(dir, { recursive: true });

				await (file as any).download(localAbsPath);
				attachments.push({ original: originalName, local: localRelPath });
				log.logInfo(`Downloaded document attachment: ${localRelPath}`);
			}
		} catch (err) {
			log.logWarning("Failed to download Telegram attachment", err instanceof Error ? err.message : String(err));
		}

		return attachments;
	}

	private logUserMessage(
		chatId: string,
		userId: string,
		messageId: string,
		text: string,
		userName: string | undefined,
		displayName: string | undefined,
		attachments: Attachment[],
	): void {
		this.logToFile(chatId, {
			date: new Date().toISOString(),
			ts: messageId,
			user: userId,
			userName,
			displayName,
			text,
			attachments,
			isBot: false,
		});
	}
}
