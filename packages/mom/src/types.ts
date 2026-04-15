import type { Attachment } from "./store.js";

// ============================================================================
// Platform-agnostic interfaces
// ============================================================================

export interface ChatEvent {
	type: "mention" | "dm";
	channel: string; // Slack channel ID / Telegram chat_id as string
	messageId: string; // Slack ts / Telegram message_id as string
	user: string; // User ID as string
	text: string;
	files?: Array<{ name?: string; url_private_download?: string; url_private?: string }>;
	/** Processed attachments with local paths (populated after logUserMessage) */
	attachments?: Attachment[];
}

export interface ChatContext {
	message: {
		text: string;
		rawText: string;
		user: string;
		userName?: string;
		channel: string;
		messageId: string;
		attachments: Array<{ local: string }>;
	};
	channelName?: string;
	channels: ChannelInfo[];
	users: UserInfo[];
	respond: (text: string, shouldLog?: boolean) => Promise<void>;
	replaceMessage: (text: string) => Promise<void>;
	respondInThread: (text: string) => Promise<void>;
	setTyping: (isTyping: boolean) => Promise<void>;
	uploadFile: (filePath: string, title?: string) => Promise<void>;
	setWorking: (working: boolean) => Promise<void>;
	deleteMessage: () => Promise<void>;
}

export interface ChatBot {
	start(): Promise<void>;
	postMessage(channel: string, text: string): Promise<string>;
	updateMessage(channel: string, messageId: string, text: string): Promise<void>;
	deleteMessage(channel: string, messageId: string): Promise<void>;
	postInThread(channel: string, parentMessageId: string, text: string): Promise<string>;
	uploadFile(channel: string, filePath: string, title?: string): Promise<void>;
	getUser(userId: string): UserInfo | undefined;
	getChannel(channelId: string): ChannelInfo | undefined;
	getAllUsers(): UserInfo[];
	getAllChannels(): ChannelInfo[];
	logToFile(channel: string, entry: object): void;
	logBotResponse(channel: string, text: string, messageId: string): void;
	enqueueEvent(event: ChatEvent): boolean;
}

export interface MomHandler {
	isRunning(channelId: string): boolean;
	handleEvent(event: ChatEvent, bot: ChatBot, isEvent?: boolean): Promise<void>;
	handleStop(channelId: string, bot: ChatBot): Promise<void>;
	handleCompact(channelId: string, bot: ChatBot): Promise<void>;
	handleNew(channelId: string, bot: ChatBot): Promise<void>;
}

export interface ChannelInfo {
	id: string;
	name: string;
}

export interface UserInfo {
	id: string;
	userName: string;
	displayName: string;
}
