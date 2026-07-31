#!/usr/bin/env node

import { join, resolve } from "path";
import { type AgentRunner, getOrCreateRunner, type Platform, resetRunner, setModel } from "./agent.js";
import { DiscordBot } from "./discord.js";
import { downloadChannel } from "./download.js";
import { createEventsWatcher } from "./events.js";
import * as log from "./log.js";
import { RocketChatBot } from "./rocketchat.js";
import { parseSandboxArg, type SandboxConfig, validateSandbox } from "./sandbox.js";
import { SlackBot as SlackBotClass } from "./slack.js";
import { ChannelStore } from "./store.js";
import { TelegramBot } from "./telegram.js";
import type { ChatBot, ChatEvent, MomHandler } from "./types.js";

// ============================================================================
// Config
// ============================================================================

const MOM_SLACK_APP_TOKEN = process.env.MOM_SLACK_APP_TOKEN;
const MOM_SLACK_BOT_TOKEN = process.env.MOM_SLACK_BOT_TOKEN;
const MOM_TELEGRAM_BOT_TOKEN = process.env.MOM_TELEGRAM_BOT_TOKEN;
const MOM_RC_URL = process.env.MOM_RC_URL;
const MOM_RC_USER = process.env.MOM_RC_USER;
const MOM_RC_PASSWORD = process.env.MOM_RC_PASSWORD;
const MOM_RC_AUTH_TOKEN = process.env.MOM_RC_AUTH_TOKEN;
const MOM_RC_USER_ID = process.env.MOM_RC_USER_ID;
const MOM_DISCORD_BOT_TOKEN = process.env.MOM_DISCORD_BOT_TOKEN;

interface ParsedArgs {
	workingDir?: string;
	sandbox: SandboxConfig;
	downloadChannel?: string;
	platform?: Platform;
	model?: string;
}

function parseArgs(): ParsedArgs {
	const args = process.argv.slice(2);
	let sandbox: SandboxConfig = { type: "host" };
	let workingDir: string | undefined;
	let downloadChannelId: string | undefined;
	let explicitPlatform: Platform | undefined;
	let modelArg: string | undefined;

	for (let i = 0; i < args.length; i++) {
		const arg = args[i];
		if (arg.startsWith("--sandbox=")) {
			sandbox = parseSandboxArg(arg.slice("--sandbox=".length));
		} else if (arg === "--sandbox") {
			sandbox = parseSandboxArg(args[++i] || "");
		} else if (arg.startsWith("--download=")) {
			downloadChannelId = arg.slice("--download=".length);
		} else if (arg === "--download") {
			downloadChannelId = args[++i];
		} else if (arg.startsWith("--platform=")) {
			explicitPlatform = arg.slice("--platform=".length) as Platform;
		} else if (arg === "--platform") {
			explicitPlatform = args[++i] as Platform;
		} else if (arg.startsWith("--model=")) {
			modelArg = arg.slice("--model=".length);
		} else if (arg === "--model") {
			modelArg = args[++i];
		} else if (!arg.startsWith("-")) {
			workingDir = arg;
		}
	}

	return {
		workingDir: workingDir ? resolve(workingDir) : undefined,
		sandbox,
		downloadChannel: downloadChannelId,
		platform: explicitPlatform,
		model: modelArg,
	};
}

// Platform detection: explicit flag > env var auto-detect
function detectPlatform(explicit?: Platform): Platform {
	if (explicit) {
		if (!["slack", "telegram", "rocketchat", "discord"].includes(explicit)) {
			console.error(`Unknown platform: ${explicit}. Use: slack, telegram, rocketchat, discord`);
			process.exit(1);
		}
		return explicit;
	}
	if (MOM_RC_URL) return "rocketchat";
	if (MOM_TELEGRAM_BOT_TOKEN) return "telegram";
	if (MOM_DISCORD_BOT_TOKEN) return "discord";
	return "slack";
}

const parsedArgs = parseArgs();

if (parsedArgs.model) {
	setModel(parsedArgs.model);
}

const platform = detectPlatform(parsedArgs.platform);

// Handle --download mode
if (parsedArgs.downloadChannel) {
	if (!MOM_SLACK_BOT_TOKEN) {
		console.error("Missing env: MOM_SLACK_BOT_TOKEN");
		process.exit(1);
	}
	await downloadChannel(parsedArgs.downloadChannel, MOM_SLACK_BOT_TOKEN);
	process.exit(0);
}

// Normal bot mode - require working dir
if (!parsedArgs.workingDir) {
	console.error(
		"Usage: mom [--platform=slack|telegram|rocketchat|discord] [--model=provider/model-id|model-id] [--sandbox=host|docker:<name>] <working-directory>",
	);
	console.error("       mom --download <channel-id>");
	process.exit(1);
}

const { workingDir, sandbox } = { workingDir: parsedArgs.workingDir, sandbox: parsedArgs.sandbox };

if (platform === "slack" && (!MOM_SLACK_APP_TOKEN || !MOM_SLACK_BOT_TOKEN)) {
	console.error("Missing env: MOM_SLACK_APP_TOKEN, MOM_SLACK_BOT_TOKEN");
	process.exit(1);
}
if (platform === "telegram" && !MOM_TELEGRAM_BOT_TOKEN) {
	console.error("Missing env: MOM_TELEGRAM_BOT_TOKEN");
	process.exit(1);
}
if (platform === "rocketchat") {
	const hasToken = MOM_RC_AUTH_TOKEN && MOM_RC_USER_ID;
	const hasLogin = MOM_RC_USER && MOM_RC_PASSWORD;
	if (!MOM_RC_URL || (!hasToken && !hasLogin)) {
		console.error(
			"Missing env: MOM_RC_URL + (MOM_RC_AUTH_TOKEN & MOM_RC_USER_ID) or (MOM_RC_USER & MOM_RC_PASSWORD)",
		);
		process.exit(1);
	}
}
if (platform === "discord" && !MOM_DISCORD_BOT_TOKEN) {
	console.error("Missing env: MOM_DISCORD_BOT_TOKEN");
	process.exit(1);
}

await validateSandbox(sandbox);

// ============================================================================
// State (per channel)
// ============================================================================

interface ChannelState {
	running: boolean;
	runner: AgentRunner;
	store: ChannelStore;
	stopRequested: boolean;
	stopMessageTs?: string;
}

const channelStates = new Map<string, ChannelState>();

function getState(channelId: string): ChannelState {
	let state = channelStates.get(channelId);
	if (!state) {
		const channelDir = join(workingDir, channelId);
		state = {
			running: false,
			runner: getOrCreateRunner(sandbox, channelId, channelDir, platform),
			store: new ChannelStore({ workingDir, botToken: MOM_SLACK_BOT_TOKEN }),
			stopRequested: false,
		};
		channelStates.set(channelId, state);
	}
	return state;
}

// ============================================================================
// Create ChatContext adapter
// ============================================================================

function createChatContext(event: ChatEvent, bot: ChatBot, state: ChannelState, isEvent?: boolean) {
	let messageTs: string | null = null;
	const threadMessageTs: string[] = [];
	let accumulatedText = "";
	let isWorking = true;
	const workingIndicator = " ...";
	let updatePromise = Promise.resolve();

	const user = bot.getUser(event.user);

	// Extract event filename for status message
	const eventFilename = isEvent ? event.text.match(/^\[EVENT:([^:]+):/)?.[1] : undefined;

	// Truncation limit (Telegram: 4K, Slack: 40K)
	const MAX_MAIN_LENGTH = platform === "telegram" ? 3500 : platform === "discord" ? 1900 : 35000;

	return {
		message: {
			text: event.text,
			rawText: event.text,
			user: event.user,
			userName: user?.userName,
			channel: event.channel,
			messageId: event.messageId,
			attachments: (event.attachments || []).map((a) => ({ local: a.local })),
		},
		channelName: bot.getChannel(event.channel)?.name,
		store: state.store,
		channels: bot.getAllChannels().map((c) => ({ id: c.id, name: c.name })),
		users: bot.getAllUsers().map((u) => ({ id: u.id, userName: u.userName, displayName: u.displayName })),

		respond: async (text: string, shouldLog = true) => {
			updatePromise = updatePromise.then(async () => {
				try {
					accumulatedText = accumulatedText ? `${accumulatedText}\n${text}` : text;

					const truncationNote = "\n\n_(message truncated, ask me to elaborate on specific parts)_";
					if (accumulatedText.length > MAX_MAIN_LENGTH) {
						accumulatedText =
							accumulatedText.substring(0, MAX_MAIN_LENGTH - truncationNote.length) + truncationNote;
					}

					const displayText = isWorking ? accumulatedText + workingIndicator : accumulatedText;

					if (messageTs) {
						await bot.updateMessage(event.channel, messageTs, displayText);
					} else {
						messageTs = await bot.postMessage(event.channel, displayText);
					}

					if (shouldLog && messageTs) {
						bot.logBotResponse(event.channel, text, messageTs);
					}
				} catch (err) {
					log.logWarning("Chat respond error", err instanceof Error ? err.message : String(err));
				}
			});
			await updatePromise;
		},

		replaceMessage: async (text: string) => {
			updatePromise = updatePromise.then(async () => {
				try {
					const truncationNote = "\n\n_(message truncated, ask me to elaborate on specific parts)_";
					if (text.length > MAX_MAIN_LENGTH) {
						accumulatedText = text.substring(0, MAX_MAIN_LENGTH - truncationNote.length) + truncationNote;
					} else {
						accumulatedText = text;
					}

					const displayText = isWorking ? accumulatedText + workingIndicator : accumulatedText;

					if (messageTs) {
						await bot.updateMessage(event.channel, messageTs, displayText);
					} else {
						messageTs = await bot.postMessage(event.channel, displayText);
					}
				} catch (err) {
					log.logWarning("Chat replaceMessage error", err instanceof Error ? err.message : String(err));
				}
			});
			await updatePromise;
		},

		respondInThread: async (text: string) => {
			updatePromise = updatePromise.then(async () => {
				try {
					if (messageTs) {
						const MAX_THREAD_LENGTH = 20000;
						let threadText = text;
						if (threadText.length > MAX_THREAD_LENGTH) {
							threadText = `${threadText.substring(0, MAX_THREAD_LENGTH - 50)}\n\n_(truncated)_`;
						}

						const ts = await bot.postInThread(event.channel, messageTs, threadText);
						threadMessageTs.push(ts);
					}
				} catch (err) {
					log.logWarning("Chat respondInThread error", err instanceof Error ? err.message : String(err));
				}
			});
			await updatePromise;
		},

		setTyping: async (isTyping: boolean) => {
			if (isTyping && !messageTs) {
				updatePromise = updatePromise.then(async () => {
					try {
						if (!messageTs) {
							accumulatedText = eventFilename ? `_Starting event: ${eventFilename}_` : "_Thinking_";
							messageTs = await bot.postMessage(event.channel, accumulatedText + workingIndicator);
						}
					} catch (err) {
						log.logWarning("Chat setTyping error", err instanceof Error ? err.message : String(err));
					}
				});
				await updatePromise;
			}
		},

		uploadFile: async (filePath: string, title?: string) => {
			await bot.uploadFile(event.channel, filePath, title);
		},

		setWorking: async (working: boolean) => {
			updatePromise = updatePromise.then(async () => {
				try {
					isWorking = working;
					if (messageTs) {
						const displayText = isWorking ? accumulatedText + workingIndicator : accumulatedText;
						await bot.updateMessage(event.channel, messageTs, displayText);
					}
				} catch (err) {
					log.logWarning("Chat setWorking error", err instanceof Error ? err.message : String(err));
				}
			});
			await updatePromise;
		},

		deleteMessage: async () => {
			updatePromise = updatePromise.then(async () => {
				// Delete thread messages first (in reverse order)
				for (let i = threadMessageTs.length - 1; i >= 0; i--) {
					try {
						await bot.deleteMessage(event.channel, threadMessageTs[i]);
					} catch {
						// Ignore errors deleting thread messages
					}
				}
				threadMessageTs.length = 0;
				// Then delete main message
				if (messageTs) {
					await bot.deleteMessage(event.channel, messageTs);
					messageTs = null;
				}
			});
			await updatePromise;
		},
	};
}

// ============================================================================
// Handler
// ============================================================================

const handler: MomHandler = {
	isRunning(channelId: string): boolean {
		const state = channelStates.get(channelId);
		return state?.running ?? false;
	},

	async handleStop(channelId: string, bot: ChatBot): Promise<void> {
		const state = channelStates.get(channelId);
		if (state?.running) {
			state.stopRequested = true;
			state.runner.abort();
			const ts = await bot.postMessage(channelId, "_Stopping..._");
			state.stopMessageTs = ts; // Save for updating later
		} else {
			await bot.postMessage(channelId, "_Nothing running_");
		}
	},

	async handleCompact(channelId: string, bot: ChatBot): Promise<void> {
		const state = channelStates.get(channelId);
		if (state?.running) {
			await bot.postMessage(channelId, "_Can't compact while running. Stop first._");
			return;
		}
		if (!state) {
			await bot.postMessage(channelId, "_No session to compact_");
			return;
		}
		const ts = await bot.postMessage(channelId, "_Compacting..._");
		const result = await state.runner.compact();
		await bot.updateMessage(channelId, ts, `_${result}_`);
	},

	async handleNew(channelId: string, bot: ChatBot): Promise<void> {
		const state = channelStates.get(channelId);
		if (state?.running) {
			await bot.postMessage(channelId, "_Can't reset while running. Stop first._");
			return;
		}
		const channelDir = join(workingDir, channelId);
		await resetRunner(channelId, channelDir);
		// Remove cached state so it gets recreated with a fresh runner
		channelStates.delete(channelId);
		await bot.postMessage(channelId, "_New session started_");
	},

	async handleModel(channelId: string, bot: ChatBot, modelReference?: string): Promise<void> {
		const state = getState(channelId);
		if (!modelReference) {
			await bot.postMessage(channelId, `_Model: ${state.runner.getModel()}_`);
			return;
		}
		if (state.running) {
			await bot.postMessage(channelId, "_Can't switch model while running. Stop first._");
			return;
		}
		const result = await state.runner.setModel(modelReference);
		await bot.postMessage(channelId, `_${result}_`);
	},

	async handleEvent(event: ChatEvent, bot: ChatBot, isEvent?: boolean): Promise<void> {
		const state = getState(event.channel);

		// Start run
		state.running = true;
		state.stopRequested = false;

		log.logInfo(`[${event.channel}] Starting run: ${event.text.substring(0, 50)}`);

		try {
			// Create context adapter
			const ctx = createChatContext(event, bot, state, isEvent);

			// Run the agent
			await ctx.setTyping(true);
			await ctx.setWorking(true);
			const result = await state.runner.run(ctx, state.store);
			await ctx.setWorking(false);

			if (result.stopReason === "aborted" && state.stopRequested) {
				if (state.stopMessageTs) {
					await bot.updateMessage(event.channel, state.stopMessageTs, "_Stopped_");
					state.stopMessageTs = undefined;
				} else {
					await bot.postMessage(event.channel, "_Stopped_");
				}
			}
		} catch (err) {
			log.logWarning(`[${event.channel}] Run error`, err instanceof Error ? err.message : String(err));
		} finally {
			state.running = false;
		}
	},
};

// ============================================================================
// Start
// ============================================================================

log.logStartup(workingDir, sandbox.type === "host" ? "host" : `docker:${sandbox.container}`);
log.logInfo(`Platform: ${platform}`);

// Shared store for attachment downloads (also used per-channel in getState)
const sharedStore = new ChannelStore({ workingDir, botToken: MOM_SLACK_BOT_TOKEN });

let chatBot: ChatBot;

if (platform === "rocketchat") {
	chatBot = new RocketChatBot(handler, {
		url: MOM_RC_URL!,
		username: MOM_RC_USER,
		password: MOM_RC_PASSWORD,
		authToken: MOM_RC_AUTH_TOKEN,
		userId: MOM_RC_USER_ID,
		workingDir,
		store: sharedStore,
	});
} else if (platform === "telegram") {
	chatBot = new TelegramBot(handler, {
		token: MOM_TELEGRAM_BOT_TOKEN!,
		workingDir,
		store: sharedStore,
	});
} else if (platform === "discord") {
	chatBot = new DiscordBot(handler, {
		token: MOM_DISCORD_BOT_TOKEN!,
		workingDir,
		store: sharedStore,
	});
} else {
	chatBot = new SlackBotClass(handler, {
		appToken: MOM_SLACK_APP_TOKEN!,
		botToken: MOM_SLACK_BOT_TOKEN!,
		workingDir,
		store: sharedStore,
	});
}

// Start events watcher
const eventsWatcher = createEventsWatcher(workingDir, chatBot);
eventsWatcher.start();

// Handle shutdown
process.on("SIGINT", () => {
	log.logInfo("Shutting down...");
	eventsWatcher.stop();
	process.exit(0);
});

process.on("SIGTERM", () => {
	log.logInfo("Shutting down...");
	eventsWatcher.stop();
	process.exit(0);
});

chatBot.start();
