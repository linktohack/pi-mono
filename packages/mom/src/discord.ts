import { AttachmentBuilder, Client, Events, GatewayIntentBits, type Message, Partials } from "discord.js";
import { appendFileSync, createWriteStream, existsSync, mkdirSync } from "fs";
import { basename, join } from "path";
import { pipeline } from "stream/promises";
import * as log from "./log.js";
import type { Attachment, ChannelStore } from "./store.js";
import type { ChannelInfo, ChatBot, ChatEvent, MomHandler, UserInfo } from "./types.js";

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
// DiscordBot
// ============================================================================

export class DiscordBot implements ChatBot {
	private client: Client;
	private handler: MomHandler;
	private workingDir: string;
	private store: ChannelStore;
	private botUserId: string | null = null;

	private users = new Map<string, UserInfo>();
	private channels = new Map<string, ChannelInfo>();
	private queues = new Map<string, ChannelQueue>();
	private soloChannels = new Set<string>();

	constructor(handler: MomHandler, config: { token: string; workingDir: string; store: ChannelStore }) {
		this.handler = handler;
		this.workingDir = config.workingDir;
		this.store = config.store;
		this.client = new Client({
			intents: [
				GatewayIntentBits.Guilds,
				GatewayIntentBits.GuildMessages,
				GatewayIntentBits.DirectMessages,
				GatewayIntentBits.MessageContent,
			],
			partials: [Partials.Channel],
		});

		this.client.once(Events.ClientReady, (readyClient) => {
			this.botUserId = readyClient.user.id;
			log.logInfo(`Discord bot: ${readyClient.user.tag} (${readyClient.user.id})`);
			log.logConnected();
		});

		this.setupMessageHandler();
		this.client.login(config.token);
	}

	// ==========================================================================
	// Public API
	// ==========================================================================

	async start(): Promise<void> {
		// Login is initiated in constructor; wait for ready
		await new Promise<void>((resolve) => {
			if (this.botUserId) {
				resolve();
			} else {
				this.client.once(Events.ClientReady, () => resolve());
			}
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
		const ch = await this.client.channels.fetch(channel);
		if (!ch?.isTextBased() || !("send" in ch)) throw new Error(`Cannot send to channel ${channel}`);
		const chunks = splitMessage(text);
		let lastId = "";
		for (const chunk of chunks) {
			const msg = await ch.send(chunk);
			lastId = msg.id;
		}
		return lastId;
	}

	async updateMessage(channel: string, messageId: string, text: string): Promise<void> {
		const ch = await this.client.channels.fetch(channel);
		if (!ch?.isTextBased() || !("messages" in ch)) return;
		try {
			const msg = await ch.messages.fetch(messageId);
			const chunks = splitMessage(text);
			await msg.edit(chunks[0]);
		} catch (err) {
			const errMsg = err instanceof Error ? err.message : String(err);
			if (!errMsg.includes("Unknown Message")) {
				log.logWarning("Discord updateMessage error", errMsg);
			}
		}
	}

	async deleteMessage(channel: string, messageId: string): Promise<void> {
		const ch = await this.client.channels.fetch(channel);
		if (!ch?.isTextBased() || !("messages" in ch)) return;
		try {
			const msg = await ch.messages.fetch(messageId);
			await msg.delete();
		} catch {
			// Ignore - message may already be deleted
		}
	}

	async postInThread(channel: string, parentMessageId: string, text: string): Promise<string> {
		const ch = await this.client.channels.fetch(channel);
		if (!ch?.isTextBased() || !("messages" in ch)) throw new Error(`Cannot thread in channel ${channel}`);

		// DM channels don't support threads - silently skip
		if (ch.isDMBased()) return "";

		const parentMsg = await ch.messages.fetch(parentMessageId);

		// Create or get existing thread
		let thread = parentMsg.thread;
		if (!thread) {
			thread = await parentMsg.startThread({ name: "Details" });
		}

		const chunks = splitMessage(text);
		let lastId = "";
		for (const chunk of chunks) {
			const msg = await thread.send(chunk);
			lastId = msg.id;
		}
		return lastId;
	}

	async uploadFile(channel: string, filePath: string, title?: string): Promise<void> {
		const ch = await this.client.channels.fetch(channel);
		if (!ch?.isTextBased() || !("send" in ch)) return;
		const attachment = new AttachmentBuilder(filePath, { name: title || basename(filePath) });
		await ch.send({ files: [attachment] });
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
		this.client.on(Events.MessageCreate, async (msg: Message) => {
			// Ignore bot messages
			if (msg.author.bot) return;

			const channelId = msg.channelId;
			const userId = msg.author.id;
			const messageId = msg.id;

			// Track user
			const userName = msg.author.username;
			const displayName = msg.author.displayName || msg.author.globalName || userName;
			if (!this.users.has(userId)) {
				this.users.set(userId, { id: userId, userName, displayName });
			}

			// Track channel
			const channelName = msg.channel.isDMBased()
				? `DM:${userName}`
				: "name" in msg.channel
					? msg.channel.name || channelId
					: channelId;
			if (!this.channels.has(channelId)) {
				this.channels.set(channelId, { id: channelId, name: channelName });
			}

			const isDM = msg.channel.isDMBased();
			const isMentioned = this.botUserId !== null && msg.mentions.has(this.botUserId);

			// Detect solo channels on first message
			if (!isDM && !this.soloChannels.has(channelId) && "members" in msg.channel && "size" in msg.channel.members) {
				if ((msg.channel.members as Map<string, unknown>).size <= 2) {
					this.soloChannels.add(channelId);
				}
			}
			const isSolo = this.soloChannels.has(channelId);

			// In guild channels, only respond to @mentions (unless solo)
			if (!isDM && !isSolo && !isMentioned) {
				this.logUserMessage(channelId, userId, messageId, msg.content, userName, displayName, []);
				return;
			}

			// Strip @mention from text
			let rawText = msg.content;
			if (this.botUserId) {
				rawText = rawText.replace(new RegExp(`<@!?${this.botUserId}>`, "g"), "").trim();
			}

			// Download attachments
			const attachments = await this.downloadAttachments(msg, channelId);

			const chatEvent: ChatEvent = {
				type: isDM ? "dm" : "mention",
				channel: channelId,
				messageId,
				user: userId,
				text: rawText,
				attachments,
			};

			// Log user message
			this.logUserMessage(channelId, userId, messageId, rawText, userName, displayName, attachments);

			// Check for stop/compact/new commands
			const cmd = chatEvent.text.toLowerCase().trim();
			if (cmd === "stop") {
				if (this.handler.isRunning(channelId)) {
					this.handler.handleStop(channelId, this);
				} else {
					this.postMessage(channelId, "*Nothing running*");
				}
				return;
			}
			if (cmd === "compact") {
				this.handler.handleCompact(channelId, this);
				return;
			}
			if (cmd === "new") {
				this.handler.handleNew(channelId, this);
				return;
			}
			// "model" reports the current model, "model <ref>" switches. A model reference never
			// contains spaces, so "model this data for me" stays a normal message.
			const modelCmd = cmd.match(/^model(?:\s+(\S+))?$/);
			if (modelCmd) {
				this.handler.handleModel(channelId, this, modelCmd[1]);
				return;
			}

			// Check if busy
			if (this.handler.isRunning(channelId)) {
				const stopHint = isDM ? "`stop`" : `\`@bot stop\``;
				this.postMessage(channelId, `*Already working. Say ${stopHint} to cancel.*`);
			} else {
				this.getQueue(channelId).enqueue(() => this.handler.handleEvent(chatEvent, this));
			}
		});
	}

	private async downloadAttachments(msg: Message, channelId: string): Promise<Attachment[]> {
		const attachments: Attachment[] = [];

		for (const [, attachment] of msg.attachments) {
			try {
				const originalName = attachment.name || "file";
				const filename = `${Date.now()}_${originalName.replace(/[^a-zA-Z0-9._-]/g, "_")}`;
				const localRelPath = `${channelId}/attachments/${filename}`;
				const localAbsPath = join(this.workingDir, localRelPath);

				const dir = join(this.workingDir, channelId, "attachments");
				if (!existsSync(dir)) mkdirSync(dir, { recursive: true });

				const response = await fetch(attachment.url);
				if (!response.ok || !response.body) {
					log.logWarning("Failed to download Discord attachment", `HTTP ${response.status}`);
					continue;
				}

				const fileStream = createWriteStream(localAbsPath);
				await pipeline(response.body as any, fileStream);

				attachments.push({ original: originalName, local: localRelPath });
				log.logInfo(`Downloaded attachment: ${localRelPath}`);
			} catch (err) {
				log.logWarning("Failed to download Discord attachment", err instanceof Error ? err.message : String(err));
			}
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

// ============================================================================
// Discord message splitting (2000 char limit)
// ============================================================================

// Returns the opener line (e.g. "```ts") of a code fence left open at the end
// of `text`, or null if all fences are balanced. Each line starting with ```
// toggles the fence state; the opener is kept so we can reopen with its language.
function openFenceAfter(text: string): string | null {
	let opener: string | null = null;
	for (const line of text.split("\n")) {
		if (line.trimStart().startsWith("```")) {
			opener = opener ? null : line.trim();
		}
	}
	return opener;
}

function splitMessage(text: string, maxLength = 2000): string[] {
	if (text.length <= maxLength) return [text];

	const chunks: string[] = [];
	let remaining = text;
	// Opener of a fence we closed at a chunk boundary and must reopen on the next chunk.
	let reopen: string | null = null;

	while (remaining.length > 0) {
		const prefix = reopen ? `${reopen}\n` : "";

		if (prefix.length + remaining.length <= maxLength) {
			chunks.push(prefix + remaining);
			break;
		}

		// Reserve room for the reopened opener and a possible closing fence ("\n```").
		const budget = maxLength - prefix.length - 4;

		// Try to split at a newline, then a space, then hard-split.
		let splitIdx = remaining.lastIndexOf("\n", budget);
		if (splitIdx < budget / 2) {
			splitIdx = remaining.lastIndexOf(" ", budget);
		}
		if (splitIdx < budget / 2) {
			splitIdx = budget;
		}

		let chunk = prefix + remaining.substring(0, splitIdx);
		remaining = remaining.substring(splitIdx).trimStart();

		// If this chunk ends inside a code fence, close it here and reopen next chunk.
		reopen = openFenceAfter(chunk);
		if (reopen) {
			chunk += "\n```";
		}

		chunks.push(chunk);
	}

	return chunks;
}
