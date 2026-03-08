import { appendFileSync, existsSync, mkdirSync } from "fs";
import { basename, join } from "path";
import WebSocket from "ws";
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
// REST API helper
// ============================================================================

interface RCConfig {
	url: string; // e.g. "http://localhost:3000"
	username?: string;
	password?: string;
	authToken?: string; // pre-existing auth token (skip login)
	userId?: string; // pre-existing user ID (skip login)
	workingDir: string;
	store: ChannelStore;
}

interface RCAuth {
	authToken: string;
	userId: string;
}

async function rcApi(baseUrl: string, method: string, path: string, auth: RCAuth | null, body?: unknown): Promise<any> {
	const headers: Record<string, string> = { "Content-Type": "application/json" };
	if (auth) {
		headers["X-Auth-Token"] = auth.authToken;
		headers["X-User-Id"] = auth.userId;
	}
	const res = await fetch(`${baseUrl}${path}`, {
		method,
		headers,
		body: body ? JSON.stringify(body) : undefined,
	});
	const json = (await res.json()) as any;
	if (!res.ok || json.success === false) {
		throw new Error(`RC API ${path}: ${json.error || res.statusText}`);
	}
	return json;
}

// ============================================================================
// RocketChatBot
// ============================================================================

export class RocketChatBot implements ChatBot {
	private config: RCConfig;
	private handler: MomHandler;
	private auth: RCAuth | null = null;
	private botUserId: string | null = null;
	private botUsername: string | null = null;
	private ws: WebSocket | null = null;
	private ddpCounter = 0;

	private users = new Map<string, UserInfo>();
	private channels = new Map<string, ChannelInfo>();
	private queues = new Map<string, ChannelQueue>();

	constructor(handler: MomHandler, config: RCConfig) {
		this.handler = handler;
		this.config = config;
	}

	// ==========================================================================
	// Public API
	// ==========================================================================

	async start(): Promise<void> {
		// 1. Authenticate (token or login)
		if (this.config.authToken && this.config.userId) {
			this.auth = { authToken: this.config.authToken, userId: this.config.userId };
			this.botUserId = this.config.userId;
			// Fetch bot username via /api/v1/me
			const meRes = await rcApi(this.config.url, "GET", "/api/v1/me", this.auth);
			this.botUsername = meRes.username || this.config.username || "bot";
			log.logInfo(`Rocket.Chat authenticated as @${this.botUsername} (${this.botUserId})`);
		} else {
			const loginRes = await rcApi(this.config.url, "POST", "/api/v1/login", null, {
				user: this.config.username,
				password: this.config.password,
			});
			this.auth = { authToken: loginRes.data.authToken, userId: loginRes.data.userId };
			this.botUserId = loginRes.data.userId;
			this.botUsername = loginRes.data.me?.username || this.config.username || "bot";
			log.logInfo(`Rocket.Chat logged in as @${this.botUsername} (${this.botUserId})`);
		}

		// Set download headers on store so attachment downloads are authenticated
		this.config.store.setDownloadHeaders({
			"X-Auth-Token": this.auth.authToken,
			"X-User-Id": this.auth.userId,
		});

		// 2. Fetch users and channels
		await Promise.all([this.fetchUsers(), this.fetchChannels()]);
		log.logInfo(`Loaded ${this.channels.size} channels, ${this.users.size} users`);

		// 3. Connect WebSocket for real-time messages
		await this.connectWebSocket();

		log.logConnected();
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
		const res = await rcApi(this.config.url, "POST", "/api/v1/chat.sendMessage", this.auth, {
			message: { rid: channel, msg: text },
		});
		return res.message._id;
	}

	async updateMessage(channel: string, messageId: string, text: string): Promise<void> {
		await rcApi(this.config.url, "POST", "/api/v1/chat.update", this.auth, {
			roomId: channel,
			msgId: messageId,
			text,
		});
	}

	async deleteMessage(channel: string, messageId: string): Promise<void> {
		await rcApi(this.config.url, "POST", "/api/v1/chat.delete", this.auth, {
			roomId: channel,
			msgId: messageId,
		});
	}

	async postInThread(channel: string, parentMessageId: string, text: string): Promise<string> {
		const res = await rcApi(this.config.url, "POST", "/api/v1/chat.sendMessage", this.auth, {
			message: { rid: channel, msg: text, tmid: parentMessageId },
		});
		return res.message._id;
	}

	async uploadFile(channel: string, filePath: string, title?: string): Promise<void> {
		const fileName = title || basename(filePath);
		const formData = new FormData();
		const fileBuffer = await import("fs/promises").then((fs) => fs.readFile(filePath));
		formData.append("file", new Blob([fileBuffer]), fileName);
		formData.append("msg", "");
		formData.append("description", fileName);

		const res = await fetch(`${this.config.url}/api/v1/rooms.upload/${channel}`, {
			method: "POST",
			headers: {
				"X-Auth-Token": this.auth!.authToken,
				"X-User-Id": this.auth!.userId,
			},
			body: formData,
		});
		if (!res.ok) {
			const json = await res.json().catch(() => ({}));
			throw new Error(`Upload failed: ${(json as any).error || res.statusText}`);
		}
	}

	logToFile(channel: string, entry: object): void {
		const dir = join(this.config.workingDir, channel);
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
	// Private - Queue
	// ==========================================================================

	private getQueue(channelId: string): ChannelQueue {
		let queue = this.queues.get(channelId);
		if (!queue) {
			queue = new ChannelQueue();
			this.queues.set(channelId, queue);
		}
		return queue;
	}

	// ==========================================================================
	// Private - WebSocket / DDP
	// ==========================================================================

	private nextId(): string {
		return (++this.ddpCounter).toString();
	}

	private connectWebSocket(): Promise<void> {
		return new Promise((resolve, reject) => {
			const wsUrl = this.config.url.replace(/^http/, "ws") + "/websocket";
			this.ws = new WebSocket(wsUrl);

			let resolved = false;

			this.ws.on("open", () => {
				// DDP connect
				this.wsSend({ msg: "connect", version: "1", support: ["1"] });
			});

			this.ws.on("message", (data) => {
				const raw = data.toString();
				if (!raw) return;

				let msg: any;
				try {
					msg = JSON.parse(raw);
				} catch {
					return;
				}

				if (msg.msg === "connected") {
					// Authenticate via DDP
					this.wsSend({
						msg: "method",
						method: "login",
						id: this.nextId(),
						params: [{ resume: this.auth!.authToken }],
					});
				} else if (msg.msg === "result" && !resolved) {
					// Login result — subscribe to rooms
					resolved = true;
					this.subscribeToRooms();
					resolve();
				} else if (msg.msg === "ping") {
					this.wsSend({ msg: "pong" });
				} else if (msg.msg === "changed" && msg.collection === "stream-room-messages") {
					this.handleRoomMessage(msg);
				}
			});

			this.ws.on("error", (err) => {
				log.logWarning("WebSocket error", err.message);
				if (!resolved) reject(err);
			});

			this.ws.on("close", () => {
				log.logWarning("WebSocket closed, reconnecting in 5s...");
				setTimeout(() => this.connectWebSocket().catch(() => {}), 5000);
			});
		});
	}

	private wsSend(data: object): void {
		if (this.ws?.readyState === WebSocket.OPEN) {
			this.ws.send(JSON.stringify(data));
		}
	}

	private subscribeToRooms(): void {
		// Subscribe to each joined channel/DM
		for (const [channelId] of this.channels) {
			this.wsSend({
				msg: "sub",
				id: this.nextId(),
				name: "stream-room-messages",
				params: [channelId, false],
			});
		}
		log.logInfo(`Subscribed to ${this.channels.size} rooms via WebSocket`);
	}

	private handleRoomMessage(msg: any): void {
		const args = msg.fields?.args;
		if (!args || args.length === 0) return;

		const message = args[0];
		// Allow messages with attachments but no text (e.g. image-only messages)
		if (!message || (!message.msg && !message.file) || !message.u) return;

		// Skip bot's own messages
		if (message.u._id === this.botUserId) return;

		// Skip system messages
		if (message.t) return;

		const roomId = message.rid;
		const userId = message.u._id;
		const messageId = message._id;
		const userName = message.u.username;
		const displayName = message.u.name || userName;

		// Track user if not known
		if (userName && !this.users.has(userId)) {
			this.users.set(userId, { id: userId, userName, displayName: displayName || userName });
		}

		const isDM = !this.channels.has(roomId) || this.channels.get(roomId)!.name.startsWith("DM:");

		// In RC file uploads, the user's text goes into attachments[].description, not message.msg
		const attachmentDescription = message.attachments?.find((a: any) => a.description)?.description || "";
		const fullText = message.msg || attachmentDescription;

		const isMentioned =
			this.botUsername !== null &&
			(fullText.includes(`@${this.botUsername}`) || message.mentions?.some((m: any) => m._id === this.botUserId));

		let text = fullText;

		// Extract file attachments from RC message
		const files: ChatEvent["files"] = [];
		if (message.file && message.attachments) {
			for (const att of message.attachments) {
				if (att.title_link) {
					files.push({
						name: att.title || message.file.name || "attachment",
						url_private_download: `${this.config.url}${att.title_link}`,
					});
				}
			}
		}

		// In channels, only respond to @mentions
		if (!isDM && !isMentioned) {
			this.logUserMessage(roomId, userId, messageId, text, userName, displayName, []);
			return;
		}

		// Strip @mention
		if (this.botUsername) {
			text = text.replace(new RegExp(`@${this.botUsername}`, "gi"), "").trim();
		}

		// Process attachments for download
		const attachments = this.config.store.processAttachments(roomId, files, messageId);

		const chatEvent: ChatEvent = {
			type: isDM ? "dm" : "mention",
			channel: roomId,
			messageId,
			user: userId,
			text,
			files,
			attachments,
		};

		// Log user message
		this.logUserMessage(roomId, userId, messageId, text, userName, displayName, attachments);

		// Check for stop command
		if (chatEvent.text.toLowerCase().trim() === "stop") {
			if (this.handler.isRunning(roomId)) {
				this.handler.handleStop(roomId, this);
			} else {
				this.postMessage(roomId, "_Nothing running_");
			}
			return;
		}

		// Check if busy
		if (this.handler.isRunning(roomId)) {
			const stopHint = isDM ? "`stop`" : `\`@${this.botUsername || "mom"} stop\``;
			this.postMessage(roomId, `_Already working. Say ${stopHint} to cancel._`);
		} else {
			this.getQueue(roomId).enqueue(() => this.handler.handleEvent(chatEvent, this));
		}
	}

	private logUserMessage(
		channelId: string,
		userId: string,
		messageId: string,
		text: string,
		userName: string | undefined,
		displayName: string | undefined,
		attachments: Attachment[],
	): void {
		this.logToFile(channelId, {
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

	// ==========================================================================
	// Private - Fetch Users/Channels
	// ==========================================================================

	private async fetchUsers(): Promise<void> {
		let offset = 0;
		const count = 200;
		let total = 0;
		do {
			const res = await rcApi(
				this.config.url,
				"GET",
				`/api/v1/users.list?offset=${offset}&count=${count}`,
				this.auth,
			);
			for (const u of res.users || []) {
				if (u._id && u.username && u.active !== false) {
					this.users.set(u._id, {
						id: u._id,
						userName: u.username,
						displayName: u.name || u.username,
					});
				}
			}
			total = res.total || 0;
			offset += count;
		} while (offset < total);
	}

	private async fetchChannels(): Promise<void> {
		// Fetch joined channels
		let offset = 0;
		const count = 200;
		let total = 0;
		do {
			const res = await rcApi(
				this.config.url,
				"GET",
				`/api/v1/channels.list.joined?offset=${offset}&count=${count}`,
				this.auth,
			);
			for (const c of res.channels || []) {
				if (c._id && c.name) {
					this.channels.set(c._id, { id: c._id, name: c.name });
				}
			}
			total = res.total || 0;
			offset += count;
		} while (offset < total);

		// Fetch DMs
		offset = 0;
		do {
			const res = await rcApi(this.config.url, "GET", `/api/v1/dm.list?offset=${offset}&count=${count}`, this.auth);
			for (const dm of res.ims || []) {
				if (dm._id) {
					// Build DM name from usernames
					const otherUsers = (dm.usernames || []).filter((u: string) => u !== this.botUsername);
					const name = otherUsers.length > 0 ? `DM:${otherUsers.join(",")}` : `DM:${dm._id}`;
					this.channels.set(dm._id, { id: dm._id, name });
				}
			}
			total = res.total || 0;
			offset += count;
		} while (offset < total);

		// Fetch groups (private channels)
		offset = 0;
		do {
			const res = await rcApi(
				this.config.url,
				"GET",
				`/api/v1/groups.list?offset=${offset}&count=${count}`,
				this.auth,
			);
			for (const g of res.groups || []) {
				if (g._id && g.name) {
					this.channels.set(g._id, { id: g._id, name: g.name });
				}
			}
			total = res.total || 0;
			offset += count;
		} while (offset < total);
	}
}
