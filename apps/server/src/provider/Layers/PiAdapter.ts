import {
  EventId,
  type PiSettings,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { makePiRpcProcess, type PiRpcWireMessage } from "../pi/PiRpcProcess.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = ProviderDriverKind.make("piAgent");

const PiState = Schema.Struct({ sessionId: Schema.String });
const decodePiState = Schema.decodeUnknownEffect(PiState);

const PiAssistantDelta = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("text_delta"),
    contentIndex: Schema.Number,
    delta: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("thinking_delta"),
    contentIndex: Schema.Number,
    delta: Schema.String,
  }),
]);
const PiKnownEvent = Schema.Union([
  Schema.Struct({ type: Schema.Literal("agent_settled") }),
  Schema.Struct({
    type: Schema.Literal("message_start"),
    message: Schema.Struct({ role: Schema.String }),
  }),
  Schema.Struct({
    type: Schema.Literal("message_update"),
    assistantMessageEvent: PiAssistantDelta,
  }),
  Schema.Struct({
    type: Schema.Literal("message_end"),
    message: Schema.Struct({
      role: Schema.String,
      content: Schema.Array(Schema.Unknown),
      stopReason: Schema.optional(Schema.String),
      errorMessage: Schema.optional(Schema.String),
    }),
  }),
  Schema.Struct({
    type: Schema.Literal("tool_execution_start"),
    toolCallId: Schema.String,
    toolName: Schema.String,
    args: Schema.Unknown,
  }),
  Schema.Struct({
    type: Schema.Literal("tool_execution_update"),
    toolCallId: Schema.String,
    toolName: Schema.String,
    args: Schema.Unknown,
    partialResult: Schema.Unknown,
  }),
  Schema.Struct({
    type: Schema.Literal("tool_execution_end"),
    toolCallId: Schema.String,
    toolName: Schema.String,
    result: Schema.Unknown,
    isError: Schema.Boolean,
  }),
]);
const decodePiKnownEvent = Schema.decodeUnknownOption(PiKnownEvent);
const PiMessageContent = Schema.Union([
  Schema.Struct({ type: Schema.Literal("text"), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("thinking"), thinking: Schema.String }),
]);
const decodePiMessageContent = Schema.decodeUnknownOption(PiMessageContent);

export interface PiAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly instanceId?: ProviderInstanceId;
}

interface PiSessionContext {
  session: ProviderSession;
  readonly rpc: Effect.Success<ReturnType<typeof makePiRpcProcess>>;
  readonly scope: Scope.Closeable;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  activeAssistantItemId: string | undefined;
  assistantMessageSequence: number;
  terminalState: "completed" | "failed" | "interrupted";
  terminalError: string | undefined;
  stopped: boolean;
}

function parsePiModelSlug(slug: string) {
  const separator = slug.indexOf("/");
  if (separator <= 0 || separator === slug.length - 1) return undefined;
  return {
    provider: slug.slice(0, separator),
    modelId: slug.slice(separator + 1),
  };
}

function piToolItemType(toolName: string) {
  const normalized = toolName.toLowerCase();
  if (normalized.includes("bash") || normalized.includes("command")) {
    return "command_execution" as const;
  }
  if (normalized.includes("edit") || normalized.includes("write") || normalized.includes("patch")) {
    return "file_change" as const;
  }
  if (normalized.includes("web") || normalized.includes("search")) {
    return "web_search" as const;
  }
  if (normalized.includes("image")) return "image_view" as const;
  return "dynamic_tool_call" as const;
}

function piAssistantItemId(turnId: TurnId, sequence: number) {
  return `pi-assistant-${turnId}-${String(sequence)}`;
}

function finalAssistantText(content: ReadonlyArray<unknown>) {
  const text: Array<string> = [];
  for (const block of content) {
    const decoded = Option.getOrUndefined(decodePiMessageContent(block));
    if (decoded?.type === "text") text.push(decoded.text);
  }
  return text.join("");
}

export const makePiAdapter = Effect.fn("makePiAdapter")(function* (
  settings: PiSettings,
  options?: PiAdapterLiveOptions,
) {
  const instanceId = options?.instanceId ?? ProviderInstanceId.make("piAgent");
  const owningScope = yield* Scope.Scope;
  const serverConfig = yield* ServerConfig;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const nativeEventLogger =
    options?.nativeEventLogger ??
    (options?.nativeEventLogPath
      ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, { stream: "native" })
      : undefined);
  const managedNativeEventLogger =
    options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;
  const sessions = new Map<ThreadId, PiSessionContext>();
  const runtimeEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const randomId = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "crypto/randomUUIDv4",
          detail: "Failed to generate a Pi runtime identifier.",
          cause,
        }),
    ),
  );
  const makeEventBase = Effect.fn("PiAdapter.makeEventBase")(function* (input: {
    readonly threadId: ThreadId;
    readonly turnId?: TurnId;
    readonly itemId?: string;
    readonly raw?: unknown;
  }) {
    return {
      eventId: EventId.make(yield* randomId),
      provider: PROVIDER,
      providerInstanceId: instanceId,
      threadId: input.threadId,
      createdAt: yield* nowIso,
      ...(input.turnId ? { turnId: input.turnId } : {}),
      ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
      ...(input.raw !== undefined
        ? { raw: { source: "pi.rpc.event" as const, payload: input.raw } }
        : {}),
    };
  });
  const publish = (event: ProviderRuntimeEvent) =>
    PubSub.publish(runtimeEvents, event).pipe(Effect.asVoid);
  const logNative = Effect.fn("PiAdapter.logNative")(function* (
    threadId: ThreadId,
    event: PiRpcWireMessage,
  ) {
    if (!nativeEventLogger) return;
    yield* nativeEventLogger
      .write(
        {
          observedAt: yield* nowIso,
          event: {
            provider: PROVIDER,
            providerInstanceId: instanceId,
            threadId,
            payload: event,
          },
        },
        threadId,
      )
      .pipe(Effect.catchCause(() => Effect.void));
  });

  const requireSession = Effect.fn("PiAdapter.requireSession")(function* (threadId: ThreadId) {
    const context = sessions.get(threadId);
    if (!context || context.stopped) {
      return yield* new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId });
    }
    return context;
  });

  const updateSession = Effect.fn("PiAdapter.updateSession")(function* (
    context: PiSessionContext,
    patch: Partial<ProviderSession>,
    clearActiveTurn = false,
  ) {
    const next = { ...context.session, ...patch, updatedAt: yield* nowIso };
    if (clearActiveTurn) {
      const { activeTurnId: _activeTurnId, ...withoutActiveTurn } = next;
      context.session = withoutActiveTurn;
      return;
    }
    context.session = next;
  });

  const stopContext = Effect.fn("PiAdapter.stopContext")(function* (
    context: PiSessionContext,
    emitExit = true,
  ) {
    if (context.stopped) return;
    context.stopped = true;
    sessions.delete(context.session.threadId);
    yield* Scope.close(context.scope, Exit.void).pipe(Effect.ignore);
    if (emitExit) {
      yield* publish({
        ...(yield* makeEventBase({ threadId: context.session.threadId })),
        type: "session.exited",
        payload: { exitKind: "graceful" },
      });
    }
  });

  const failContext = Effect.fn("PiAdapter.failContext")(function* (
    context: PiSessionContext,
    cause: unknown,
  ) {
    if (context.stopped) return;
    const turnId = context.activeTurnId;
    context.stopped = true;
    sessions.delete(context.session.threadId);
    const message = cause instanceof Error ? cause.message : "Pi Agent RPC process failed.";
    if (turnId) {
      yield* publish({
        ...(yield* makeEventBase({ threadId: context.session.threadId, turnId })),
        type: "turn.completed",
        payload: { state: "failed", errorMessage: message },
      });
    }
    yield* publish({
      ...(yield* makeEventBase({
        threadId: context.session.threadId,
        ...(turnId ? { turnId } : {}),
      })),
      type: "runtime.error",
      payload: { message, class: "transport_error", detail: cause },
    });
    yield* publish({
      ...(yield* makeEventBase({
        threadId: context.session.threadId,
        ...(turnId ? { turnId } : {}),
      })),
      type: "session.exited",
      payload: { reason: message, recoverable: false, exitKind: "error" },
    });
    yield* Scope.close(context.scope, Exit.void).pipe(Effect.ignore);
  });

  const beginAssistantMessage = (context: PiSessionContext, turnId: TurnId) => {
    context.assistantMessageSequence += 1;
    const itemId = piAssistantItemId(turnId, context.assistantMessageSequence);
    context.activeAssistantItemId = itemId;
    return itemId;
  };

  const handleEvent = Effect.fn("PiAdapter.handleEvent")(function* (
    context: PiSessionContext,
    rawEvent: PiRpcWireMessage,
  ) {
    yield* logNative(context.session.threadId, rawEvent);
    const event = Option.getOrUndefined(decodePiKnownEvent(rawEvent));
    if (!event) return;
    const turnId = context.activeTurnId;

    switch (event.type) {
      case "message_start": {
        if (!turnId || event.message.role !== "assistant") return;
        beginAssistantMessage(context, turnId);
        return;
      }
      case "message_update": {
        if (!turnId) return;
        const delta = event.assistantMessageEvent;
        const itemId = context.activeAssistantItemId ?? beginAssistantMessage(context, turnId);
        yield* publish({
          ...(yield* makeEventBase({
            threadId: context.session.threadId,
            turnId,
            itemId,
            raw: rawEvent,
          })),
          type: "content.delta",
          payload: {
            streamKind: delta.type === "thinking_delta" ? "reasoning_text" : "assistant_text",
            delta: delta.delta,
          },
        });
        return;
      }
      case "message_end": {
        if (!turnId || event.message.role !== "assistant") return;
        const itemId = context.activeAssistantItemId ?? beginAssistantMessage(context, turnId);
        const detail = finalAssistantText(event.message.content);
        if (event.message.stopReason === "error") {
          context.terminalState = "failed";
          context.terminalError = event.message.errorMessage ?? "Pi Agent turn failed.";
        } else if (event.message.stopReason === "aborted") {
          context.terminalState = "interrupted";
        }
        yield* publish({
          ...(yield* makeEventBase({
            threadId: context.session.threadId,
            turnId,
            itemId,
            raw: rawEvent,
          })),
          type: "item.completed",
          payload: {
            itemType: "assistant_message",
            status: "completed",
            title: "Assistant message",
            ...(detail ? { detail } : {}),
          },
        });
        context.activeAssistantItemId = undefined;
        return;
      }
      case "tool_execution_start": {
        if (!turnId) return;
        yield* publish({
          ...(yield* makeEventBase({
            threadId: context.session.threadId,
            turnId,
            itemId: event.toolCallId,
            raw: rawEvent,
          })),
          type: "item.started",
          payload: {
            itemType: piToolItemType(event.toolName),
            status: "inProgress",
            title: event.toolName,
            data: { tool: event.toolName, args: event.args },
          },
        });
        return;
      }
      case "tool_execution_update": {
        if (!turnId) return;
        yield* publish({
          ...(yield* makeEventBase({
            threadId: context.session.threadId,
            turnId,
            itemId: event.toolCallId,
            raw: rawEvent,
          })),
          type: "item.updated",
          payload: {
            itemType: piToolItemType(event.toolName),
            status: "inProgress",
            title: event.toolName,
            data: { tool: event.toolName, args: event.args, result: event.partialResult },
          },
        });
        return;
      }
      case "tool_execution_end": {
        if (!turnId) return;
        context.turns.find((turn) => turn.id === turnId)?.items.push(event);
        yield* publish({
          ...(yield* makeEventBase({
            threadId: context.session.threadId,
            turnId,
            itemId: event.toolCallId,
            raw: rawEvent,
          })),
          type: "item.completed",
          payload: {
            itemType: piToolItemType(event.toolName),
            status: event.isError ? "failed" : "completed",
            title: event.toolName,
            data: { tool: event.toolName, result: event.result },
          },
        });
        return;
      }
      case "agent_settled": {
        if (!turnId) return;
        context.activeTurnId = undefined;
        context.activeAssistantItemId = undefined;
        yield* updateSession(context, { status: "ready" }, true);
        if (context.terminalState === "interrupted") {
          yield* publish({
            ...(yield* makeEventBase({
              threadId: context.session.threadId,
              turnId,
              raw: rawEvent,
            })),
            type: "turn.aborted",
            payload: { reason: "Pi Agent turn was interrupted." },
          });
          return;
        }
        yield* publish({
          ...(yield* makeEventBase({
            threadId: context.session.threadId,
            turnId,
            raw: rawEvent,
          })),
          type: "turn.completed",
          payload:
            context.terminalState === "failed"
              ? {
                  state: "failed",
                  errorMessage: context.terminalError ?? "Pi Agent turn failed.",
                }
              : { state: "completed" },
        });
        return;
      }
    }
  });

  const startEventPump = Effect.fn("PiAdapter.startEventPump")(function* (
    context: PiSessionContext,
  ) {
    yield* context.rpc.events.pipe(
      Stream.runForEach((event) => handleEvent(context, event)),
      Effect.catch((cause) => failContext(context, cause)),
      Effect.catchCause((cause) =>
        Effect.logError("Failed to publish Pi Agent terminal events.", { cause }),
      ),
      Effect.forkIn(context.scope),
    );
  });

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      const active = [...sessions.values()];
      yield* Effect.forEach(active, (context) => stopContext(context, false), {
        concurrency: "unbounded",
        discard: true,
      });
      if (managedNativeEventLogger) yield* managedNativeEventLogger.close();
      yield* PubSub.shutdown(runtimeEvents);
    }).pipe(Effect.ignoreCause),
  );

  const startSession: PiAdapterShape["startSession"] = Effect.fn("PiAdapter.startSession")(
    function* (input) {
      if (input.provider !== undefined && input.provider !== PROVIDER) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
        });
      }
      if (input.providerInstanceId !== undefined && input.providerInstanceId !== instanceId) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: `Pi Agent instance '${instanceId}' cannot start a session routed to '${input.providerInstanceId}'.`,
        });
      }
      const cwd = path.resolve(input.cwd?.trim() || serverConfig.cwd);
      const selection = input.modelSelection;
      if (selection && selection.instanceId !== instanceId) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: `Pi Agent model selection is bound to '${selection.instanceId}', expected '${instanceId}'.`,
        });
      }
      const nativeModel = selection ? parsePiModelSlug(selection.model) : undefined;
      if (selection && !nativeModel) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: "Pi Agent model selection must use the 'provider/model' format.",
        });
      }
      const existing = sessions.get(input.threadId);
      if (existing) yield* stopContext(existing, false);

      const sessionScope = yield* Scope.make("sequential");
      yield* Scope.addFinalizer(
        owningScope,
        Scope.close(sessionScope, Exit.void).pipe(Effect.ignore),
      );
      const rpcExit = yield* makePiRpcProcess({
        command: settings.binaryPath || "pi",
        args: ["--mode", "rpc"],
        cwd,
        ...(options?.environment ? { environment: options.environment } : {}),
      }).pipe(
        Effect.provideService(Scope.Scope, sessionScope),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
        Effect.exit,
      );
      if (Exit.isFailure(rpcExit)) {
        yield* Scope.close(sessionScope, Exit.void).pipe(Effect.ignore);
        return yield* new ProviderAdapterProcessError({
          provider: PROVIDER,
          threadId: input.threadId,
          detail: "Failed to start the Pi Agent RPC process.",
          cause: rpcExit.cause,
        });
      }
      const rpc = rpcExit.value;
      const configuredExit = yield* Effect.gen(function* () {
        if (nativeModel) {
          yield* rpc
            .request({
              type: "set_model",
              provider: nativeModel.provider,
              modelId: nativeModel.modelId,
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "set_model",
                    detail: cause.message,
                    cause,
                  }),
              ),
            );
        }
        const stateResponse = yield* rpc.request({ type: "get_state" }).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "get_state",
                detail: cause.message,
                cause,
              }),
          ),
        );
        return yield* decodePiState(stateResponse.data).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "get_state",
                detail: "Pi Agent returned an invalid session state.",
                cause,
              }),
          ),
        );
      }).pipe(Effect.exit);
      if (Exit.isFailure(configuredExit)) {
        yield* Scope.close(sessionScope, Exit.void).pipe(Effect.ignore);
        return yield* Effect.failCause(configuredExit.cause);
      }
      const state = configuredExit.value;
      const now = yield* nowIso;
      const session: ProviderSession = {
        provider: PROVIDER,
        providerInstanceId: instanceId,
        status: "ready",
        runtimeMode: input.runtimeMode,
        cwd,
        ...(selection ? { model: selection.model } : {}),
        threadId: input.threadId,
        createdAt: now,
        updatedAt: now,
      };
      const context: PiSessionContext = {
        session,
        rpc,
        scope: sessionScope,
        turns: [],
        activeTurnId: undefined,
        activeAssistantItemId: undefined,
        assistantMessageSequence: 0,
        terminalState: "completed",
        terminalError: undefined,
        stopped: false,
      };
      sessions.set(input.threadId, context);
      yield* startEventPump(context);
      yield* publish({
        ...(yield* makeEventBase({ threadId: input.threadId })),
        type: "session.started",
        payload: { message: "Pi Agent session started" },
      });
      yield* publish({
        ...(yield* makeEventBase({ threadId: input.threadId })),
        type: "thread.started",
        payload: { providerThreadId: state.sessionId },
      });
      return session;
    },
  );

  const sendTurn: PiAdapterShape["sendTurn"] = Effect.fn("PiAdapter.sendTurn")(function* (input) {
    const context = yield* requireSession(input.threadId);
    if (context.activeTurnId) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "sendTurn",
        issue: "Pi Agent is already running a turn for this thread.",
      });
    }
    if (!input.input?.trim()) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "sendTurn",
        issue: "A non-empty text prompt is required.",
      });
    }
    if (input.attachments && input.attachments.length > 0) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "sendTurn",
        issue: "Pi Agent attachments are not available in this provider version.",
      });
    }
    if (
      input.modelSelection &&
      (input.modelSelection.instanceId !== instanceId ||
        input.modelSelection.model !== context.session.model)
    ) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "sendTurn",
        issue: "Changing Pi Agent models inside a session is not available yet.",
      });
    }

    const turnId = TurnId.make(`pi-turn-${yield* randomId}`);
    context.activeTurnId = turnId;
    context.activeAssistantItemId = undefined;
    context.assistantMessageSequence = 0;
    context.terminalState = "completed";
    context.terminalError = undefined;
    context.turns.push({ id: turnId, items: [] });
    yield* updateSession(context, { status: "running", activeTurnId: turnId });
    yield* publish({
      ...(yield* makeEventBase({ threadId: input.threadId, turnId })),
      type: "turn.started",
      payload: context.session.model ? { model: context.session.model } : {},
    });
    const promptExit = yield* context.rpc
      .request({ type: "prompt", message: input.input.trim() })
      .pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "prompt",
              detail: cause.message,
              cause,
            }),
        ),
        Effect.exit,
      );
    if (Exit.isFailure(promptExit)) {
      context.activeTurnId = undefined;
      context.activeAssistantItemId = undefined;
      yield* updateSession(context, { status: "ready" }, true);
      const cause = promptExit.cause;
      yield* publish({
        ...(yield* makeEventBase({
          threadId: input.threadId,
          turnId,
          raw: { type: "prompt_rejected" },
        })),
        type: "turn.completed",
        payload: {
          state: "failed",
          errorMessage: "Pi Agent rejected the prompt before starting work.",
        },
      });
      return yield* Effect.failCause(cause);
    }
    return { threadId: input.threadId, turnId };
  });

  const interruptTurn: PiAdapterShape["interruptTurn"] = Effect.fn("PiAdapter.interruptTurn")(
    function* (threadId) {
      const context = yield* requireSession(threadId);
      if (!context.activeTurnId) return;
      context.terminalState = "interrupted";
      yield* context.rpc.request({ type: "abort" }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "abort",
              detail: cause.message,
              cause,
            }),
        ),
      );
    },
  );

  const unsupportedRequest = (operation: string) =>
    Effect.fail(
      new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation,
        issue: "Pi Agent does not expose permission or structured input responses in this slice.",
      }),
    );

  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "unsupported" },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest: (_threadId, _requestId, _decision) => unsupportedRequest("respondToRequest"),
    respondToUserInput: (_threadId, _requestId, _answers) =>
      unsupportedRequest("respondToUserInput"),
    stopSession: (threadId) => requireSession(threadId).pipe(Effect.flatMap(stopContext)),
    listSessions: () => Effect.succeed([...sessions.values()].map((context) => context.session)),
    hasSession: (threadId) => Effect.succeed(sessions.has(threadId)),
    readThread: (threadId) =>
      requireSession(threadId).pipe(Effect.map((context) => ({ threadId, turns: context.turns }))),
    rollbackThread: (_threadId, _numTurns) => unsupportedRequest("rollbackThread"),
    stopAll: () =>
      Effect.forEach([...sessions.values()], (context) => stopContext(context), {
        concurrency: "unbounded",
        discard: true,
      }),
    get streamEvents() {
      return Stream.fromPubSub(runtimeEvents);
    },
  } satisfies PiAdapterShape;
});
