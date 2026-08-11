import {
  EventId,
  type ModelSelection,
  type PiSettings,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import {
  decodePiThinkingLevel,
  parsePiModelSlug,
  PI_THINKING_LEVEL_OPTION_ID,
} from "../pi/PiModel.ts";
import { makePiRpcProcess, type PiRpcWireMessage } from "../pi/PiRpcProcess.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = ProviderDriverKind.make("piAgent");
const PI_RESUME_SCHEMA_VERSION = 1;

export const PiResumeCursor = Schema.Struct({
  schemaVersion: Schema.Literal(PI_RESUME_SCHEMA_VERSION),
  sessionFile: Schema.String,
  sessionId: Schema.String,
});
type PiResumeCursor = typeof PiResumeCursor.Type;
const decodePiResumeCursor = Schema.decodeUnknownEffect(PiResumeCursor);

const PiState = Schema.Struct({ sessionFile: Schema.String, sessionId: Schema.String });
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
  Schema.Struct({ type: Schema.Literal("agent_start") }),
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
type PiKnownEvent = typeof PiKnownEvent.Type;
const decodePiKnownEvent = Schema.decodeUnknownOption(PiKnownEvent);
const PiMessageContent = Schema.Union([
  Schema.Struct({ type: Schema.Literal("text"), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("thinking"), thinking: Schema.String }),
]);
const decodePiMessageContent = Schema.decodeUnknownOption(PiMessageContent);
const PiSnapshotMessage = Schema.StructWithRest(Schema.Struct({ role: Schema.String }), [
  Schema.Record(Schema.String, Schema.Unknown),
]);
const PiSnapshotEntryFields = {
  id: Schema.String,
  parentId: Schema.NullOr(Schema.String),
};
const PiSnapshotEntry = Schema.StructWithRest(Schema.Struct(PiSnapshotEntryFields), [
  Schema.Record(Schema.String, Schema.Unknown),
]);
const PiSnapshotMessageEntry = Schema.StructWithRest(
  Schema.Struct({
    ...PiSnapshotEntryFields,
    type: Schema.Literal("message"),
    message: PiSnapshotMessage,
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);
const decodePiSnapshotEntry = Schema.decodeUnknownOption(PiSnapshotEntry);
const decodePiSnapshotMessageEntry = Schema.decodeUnknownOption(PiSnapshotMessageEntry);
const PiEntriesResponse = Schema.Struct({
  entries: Schema.Array(Schema.Unknown),
  leafId: Schema.NullOr(Schema.String),
});
const decodePiEntriesResponse = Schema.decodeUnknownEffect(PiEntriesResponse);

export interface PiAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly instanceId?: ProviderInstanceId;
}

interface PiBufferedEvent {
  readonly event: PiKnownEvent;
  readonly raw: PiRpcWireMessage;
}

interface PiActiveTurn {
  readonly id: TurnId;
  phase: "starting" | "running";
  terminalState: "completed" | "failed" | "interrupted";
  terminalError: string | undefined;
  control:
    | { readonly _tag: "Active" }
    | {
        readonly _tag: "Aborting";
        bufferedEvents: Array<PiBufferedEvent>;
      };
}

interface PiSessionContext {
  session: ProviderSession;
  readonly rpc: Effect.Success<ReturnType<typeof makePiRpcProcess>>;
  readonly scope: Scope.Closeable;
  readonly lifecycleMutex: Semaphore.Semaphore;
  activeTurn: PiActiveTurn | undefined;
  activeAssistantItemId: string | undefined;
  assistantMessageSequence: number;
  stopped: boolean;
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
  const fileSystem = yield* FileSystem.FileSystem;
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

  const applyModelSelection = Effect.fn("PiAdapter.applyModelSelection")(function* (
    rpc: PiSessionContext["rpc"],
    selection: ModelSelection,
    currentModel: string | undefined,
  ) {
    if (selection.instanceId !== instanceId) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "applyModelSelection",
        issue: `Pi Agent model selection is bound to '${selection.instanceId}', expected '${instanceId}'.`,
      });
    }
    const nativeModel = parsePiModelSlug(selection.model);
    if (!nativeModel) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "applyModelSelection",
        issue: "Pi Agent model selection must use the 'provider/model' format.",
      });
    }
    const rawThinkingLevel = getModelSelectionStringOptionValue(
      selection,
      PI_THINKING_LEVEL_OPTION_ID,
    );
    const thinkingLevel =
      rawThinkingLevel === undefined
        ? undefined
        : Option.getOrUndefined(decodePiThinkingLevel(rawThinkingLevel));
    if (rawThinkingLevel !== undefined && thinkingLevel === undefined) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "applyModelSelection",
        issue: `Unsupported Pi Agent thinking level '${rawThinkingLevel}'.`,
      });
    }
    if (selection.model !== currentModel) {
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
    // Without an explicit choice, Pi owns model-specific clamping and defaults.
    if (thinkingLevel !== undefined) {
      yield* rpc.request({ type: "set_thinking_level", level: thinkingLevel }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "set_thinking_level",
              detail: cause.message,
              cause,
            }),
        ),
      );
    }
    return selection.model;
  });

  const requireSession = Effect.fn("PiAdapter.requireSession")(function* (threadId: ThreadId) {
    const context = sessions.get(threadId);
    if (!context || context.stopped) {
      return yield* new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId });
    }
    return context;
  });

  const decodeResumeCursor = Effect.fn("PiAdapter.decodeResumeCursor")(function* (
    resumeCursor: unknown,
  ) {
    const cursor = yield* decodePiResumeCursor(resumeCursor).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "Pi Agent resume state is invalid or uses an unsupported schema version.",
            cause,
          }),
      ),
    );
    if (!cursor.sessionId.trim() || !path.isAbsolute(cursor.sessionFile)) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "startSession",
        issue: "Pi Agent resume state requires a native session ID and absolute session file.",
      });
    }
    return { ...cursor, sessionFile: path.resolve(cursor.sessionFile) };
  });

  const readNativeCursor = Effect.fn("PiAdapter.readNativeCursor")(function* (
    rpc: PiSessionContext["rpc"],
    method: string,
  ) {
    const stateResponse = yield* rpc.request({ type: "get_state" }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method,
            detail: cause.message,
            cause,
          }),
      ),
    );
    const state = yield* decodePiState(stateResponse.data).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method,
            detail: "Pi Agent returned an invalid persistent session state.",
            cause,
          }),
      ),
    );
    if (!state.sessionId.trim() || !path.isAbsolute(state.sessionFile)) {
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method,
        detail: "Pi Agent did not report an absolute native session file and session ID.",
      });
    }
    return {
      schemaVersion: PI_RESUME_SCHEMA_VERSION,
      sessionFile: path.resolve(state.sessionFile),
      sessionId: state.sessionId,
    } satisfies PiResumeCursor;
  });

  const verifyNativeCursor = Effect.fn("PiAdapter.verifyNativeCursor")(function* (
    expected: PiResumeCursor,
    actual: PiResumeCursor,
    method: string,
  ) {
    if (expected.sessionFile !== actual.sessionFile || expected.sessionId !== actual.sessionId) {
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method,
        detail: "Pi Agent opened a different native session than the recorded resume state.",
      });
    }
    return actual;
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

  const refreshNativeCursor = Effect.fn("PiAdapter.refreshNativeCursor")(function* (
    context: PiSessionContext,
  ) {
    const previousCursor = yield* decodeResumeCursor(context.session.resumeCursor);
    const refreshedCursor = yield* readNativeCursor(context.rpc, "get_state").pipe(
      Effect.flatMap((currentCursor) =>
        verifyNativeCursor(previousCursor, currentCursor, "session/state"),
      ),
    );
    yield* updateSession(context, { resumeCursor: refreshedCursor });
    return refreshedCursor;
  });

  const ensureCurrentContext = Effect.fn("PiAdapter.ensureCurrentContext")(function* (
    context: PiSessionContext,
  ) {
    if (context.stopped || sessions.get(context.session.threadId) !== context) {
      return yield* new ProviderAdapterSessionNotFoundError({
        provider: PROVIDER,
        threadId: context.session.threadId,
      });
    }
  });

  const finalizeTurn = Effect.fn("PiAdapter.finalizeTurn")(function* (
    context: PiSessionContext,
    turn: PiActiveTurn,
    input?: {
      readonly state?: "completed" | "failed" | "interrupted";
      readonly errorMessage?: string;
      readonly reason?: string;
      readonly raw?: PiRpcWireMessage;
    },
  ) {
    if (context.activeTurn !== turn) return false;

    const state = input?.state ?? turn.terminalState;
    const errorMessage = input?.errorMessage ?? turn.terminalError;
    context.activeTurn = undefined;
    context.activeAssistantItemId = undefined;
    yield* updateSession(context, { status: "ready" }, true);

    if (state === "interrupted") {
      yield* publish({
        ...(yield* makeEventBase({
          threadId: context.session.threadId,
          turnId: turn.id,
          ...(input?.raw ? { raw: input.raw } : {}),
        })),
        type: "turn.aborted",
        payload: { reason: input?.reason ?? "Pi Agent turn was interrupted." },
      });
      return true;
    }

    yield* publish({
      ...(yield* makeEventBase({
        threadId: context.session.threadId,
        turnId: turn.id,
        ...(input?.raw ? { raw: input.raw } : {}),
      })),
      type: "turn.completed",
      payload:
        state === "failed"
          ? { state: "failed", errorMessage: errorMessage ?? "Pi Agent turn failed." }
          : { state: "completed" },
    });
    return true;
  });

  const stopContext = Effect.fn("PiAdapter.stopContext")(function* (
    context: PiSessionContext,
    emitExit = true,
  ) {
    const claimed = yield* context.lifecycleMutex.withPermit(
      Effect.gen(function* () {
        if (context.stopped) return false;
        const turn = context.activeTurn;
        if (turn) {
          yield* finalizeTurn(context, turn, {
            state: "interrupted",
            reason: "Pi Agent session stopped while the turn was active.",
          });
        }
        context.stopped = true;
        sessions.delete(context.session.threadId);
        return true;
      }),
    );
    if (!claimed) return;

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
    const message = cause instanceof Error ? cause.message : "Pi Agent RPC process failed.";
    const claimed = yield* context.lifecycleMutex.withPermit(
      Effect.gen(function* () {
        if (context.stopped) return { claimed: false as const };
        const turn = context.activeTurn;
        if (turn) {
          yield* finalizeTurn(context, turn, { state: "failed", errorMessage: message });
        }
        context.stopped = true;
        sessions.delete(context.session.threadId);
        return { claimed: true as const, turnId: turn?.id };
      }),
    );
    if (!claimed.claimed) return;

    yield* publish({
      ...(yield* makeEventBase({
        threadId: context.session.threadId,
        ...(claimed.turnId ? { turnId: claimed.turnId } : {}),
      })),
      type: "runtime.error",
      payload: { message, class: "transport_error", detail: cause },
    });
    yield* publish({
      ...(yield* makeEventBase({
        threadId: context.session.threadId,
        ...(claimed.turnId ? { turnId: claimed.turnId } : {}),
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

  const processKnownEvent = Effect.fn("PiAdapter.processKnownEvent")(function* (
    context: PiSessionContext,
    event: PiKnownEvent,
    rawEvent: PiRpcWireMessage,
  ) {
    if (context.stopped) return;
    const turn = context.activeTurn;
    if (!turn) return;

    if (turn.control._tag === "Aborting") {
      turn.control.bufferedEvents.push({ event, raw: rawEvent });
      return;
    }

    // Pi events do not carry a T3 turn ID. Until the next native agent_start,
    // events still queued from a settled or interrupted turn stay fenced out.
    if (event.type === "agent_start") {
      if (turn.phase === "starting") turn.phase = "running";
      return;
    }
    if (turn.phase === "starting") return;

    const turnId = turn.id;
    switch (event.type) {
      case "message_start": {
        if (event.message.role !== "assistant") return;
        beginAssistantMessage(context, turnId);
        return;
      }
      case "message_update": {
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
        if (event.message.role !== "assistant") return;
        const itemId = context.activeAssistantItemId ?? beginAssistantMessage(context, turnId);
        const detail = finalAssistantText(event.message.content);
        if (event.message.stopReason === "error") {
          turn.terminalState = "failed";
          turn.terminalError = event.message.errorMessage ?? "Pi Agent turn failed.";
        } else if (event.message.stopReason === "aborted") {
          turn.terminalState = "interrupted";
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
        yield* finalizeTurn(context, turn, { raw: rawEvent });
        return;
      }
    }
  });

  const handleEvent = Effect.fn("PiAdapter.handleEvent")(function* (
    context: PiSessionContext,
    rawEvent: PiRpcWireMessage,
  ) {
    yield* logNative(context.session.threadId, rawEvent);
    const event = Option.getOrUndefined(decodePiKnownEvent(rawEvent));
    if (!event) return;
    yield* context.lifecycleMutex.withPermit(processKnownEvent(context, event, rawEvent));
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
      const resumeCursor =
        input.resumeCursor === undefined
          ? undefined
          : yield* decodeResumeCursor(input.resumeCursor);
      if (resumeCursor) {
        const exists = yield* fileSystem.exists(resumeCursor.sessionFile).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "session/resume",
                detail: `Failed to inspect recorded native session '${resumeCursor.sessionFile}'.`,
                cause,
              }),
          ),
        );
        if (!exists) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/resume",
            detail: `Recorded native session does not exist: ${resumeCursor.sessionFile}`,
          });
        }
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
        args: ["--mode", "rpc", ...(resumeCursor ? ["--session", resumeCursor.sessionFile] : [])],
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
          detail: resumeCursor
            ? "Failed to resume the recorded Pi Agent native session."
            : "Failed to start the Pi Agent RPC process.",
          cause: rpcExit.cause,
        });
      }
      const rpc = rpcExit.value;
      const configuredExit = yield* Effect.gen(function* () {
        const observedCursor = yield* readNativeCursor(rpc, "get_state");
        const currentCursor = resumeCursor
          ? yield* verifyNativeCursor(resumeCursor, observedCursor, "session/resume")
          : observedCursor;
        if (selection) yield* applyModelSelection(rpc, selection, undefined);
        return currentCursor;
      }).pipe(Effect.exit);
      if (Exit.isFailure(configuredExit)) {
        yield* Scope.close(sessionScope, Exit.void).pipe(Effect.ignore);
        if (resumeCursor) {
          return yield* new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId: input.threadId,
            detail: "Failed to resume the recorded Pi Agent native session.",
            cause: configuredExit.cause,
          });
        }
        return yield* Effect.failCause(configuredExit.cause);
      }
      const currentCursor = configuredExit.value;
      const now = yield* nowIso;
      const session: ProviderSession = {
        provider: PROVIDER,
        providerInstanceId: instanceId,
        status: "ready",
        runtimeMode: input.runtimeMode,
        cwd,
        ...(selection ? { model: selection.model } : {}),
        threadId: input.threadId,
        resumeCursor: currentCursor,
        createdAt: now,
        updatedAt: now,
      };
      const context: PiSessionContext = {
        session,
        rpc,
        scope: sessionScope,
        lifecycleMutex: yield* Semaphore.make(1),
        activeTurn: undefined,
        activeAssistantItemId: undefined,
        assistantMessageSequence: 0,
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
        payload: { providerThreadId: currentCursor.sessionId },
      });
      return session;
    },
  );

  const sendTurn: PiAdapterShape["sendTurn"] = Effect.fn("PiAdapter.sendTurn")(function* (input) {
    const context = yield* requireSession(input.threadId);
    const message = input.input?.trim();
    if (!message) {
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
    if (input.modelSelection) {
      const model = yield* applyModelSelection(
        context.rpc,
        input.modelSelection,
        context.session.model,
      );
      yield* updateSession(context, { model });
    }

    return yield* context.lifecycleMutex.withPermit(
      Effect.gen(function* () {
        yield* ensureCurrentContext(context);
        const activeTurn = context.activeTurn;
        if (activeTurn) {
          if (activeTurn.control._tag === "Aborting") {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Pi Agent is interrupting the active turn for this thread.",
            });
          }
          yield* context.rpc.request({ type: "steer", message }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "steer",
                  detail: cause.message,
                  cause,
                }),
            ),
          );
          const resumeCursor = yield* refreshNativeCursor(context);
          return { threadId: input.threadId, turnId: activeTurn.id, resumeCursor };
        }

        const turnId = TurnId.make(`pi-turn-${yield* randomId}`);
        const turn: PiActiveTurn = {
          id: turnId,
          phase: "starting",
          terminalState: "completed",
          terminalError: undefined,
          control: { _tag: "Active" },
        };
        context.activeTurn = turn;
        context.activeAssistantItemId = undefined;
        context.assistantMessageSequence = 0;
        yield* updateSession(context, { status: "running", activeTurnId: turnId });
        yield* publish({
          ...(yield* makeEventBase({ threadId: input.threadId, turnId })),
          type: "turn.started",
          payload: context.session.model ? { model: context.session.model } : {},
        });

        const promptExit = yield* context.rpc.request({ type: "prompt", message }).pipe(
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
          yield* finalizeTurn(context, turn, {
            state: "failed",
            errorMessage: "Pi Agent rejected the prompt before starting work.",
            raw: { type: "prompt_rejected" },
          });
          return yield* Effect.failCause(promptExit.cause);
        }
        const resumeCursor = yield* refreshNativeCursor(context);
        return { threadId: input.threadId, turnId, resumeCursor };
      }),
    );
  });

  const interruptTurn: PiAdapterShape["interruptTurn"] = Effect.fn("PiAdapter.interruptTurn")(
    function* (threadId) {
      const context = yield* requireSession(threadId);
      const turn = yield* context.lifecycleMutex.withPermit(
        Effect.gen(function* () {
          yield* ensureCurrentContext(context);
          const activeTurn = context.activeTurn;
          if (!activeTurn || activeTurn.control._tag === "Aborting") return undefined;
          activeTurn.control = {
            _tag: "Aborting",
            bufferedEvents: [],
          };
          return activeTurn;
        }),
      );
      if (!turn) return;

      const abortExit = yield* context.rpc.request({ type: "abort" }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "abort",
              detail: cause.message,
              cause,
            }),
        ),
        Effect.exit,
      );

      if (Exit.isSuccess(abortExit)) {
        yield* context.lifecycleMutex.withPermit(
          Effect.gen(function* () {
            yield* ensureCurrentContext(context);
            if (context.activeTurn !== turn || turn.control._tag !== "Aborting") return;
            yield* finalizeTurn(context, turn, { state: "interrupted" });
          }),
        );
        return;
      }

      yield* context.lifecycleMutex.withPermit(
        Effect.gen(function* () {
          if (context.stopped || context.activeTurn !== turn || turn.control._tag !== "Aborting") {
            return;
          }
          const bufferedEvents = turn.control.bufferedEvents;
          turn.control = { _tag: "Active" };
          for (const buffered of bufferedEvents) {
            yield* processKnownEvent(context, buffered.event, buffered.raw);
          }
        }),
      );
      return yield* Effect.failCause(abortExit.cause);
    },
  );

  const readThread: PiAdapterShape["readThread"] = Effect.fn("PiAdapter.readThread")(
    function* (threadId) {
      const context = yield* requireSession(threadId);
      const response = yield* context.rpc.request({ type: "get_entries" }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "get_entries",
              detail: cause.message,
              cause,
            }),
        ),
      );
      const data = yield* decodePiEntriesResponse(response.data).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "get_entries",
              detail: "Pi Agent returned invalid native session entries.",
              cause,
            }),
        ),
      );
      const entriesById = new Map(
        data.entries.flatMap((rawEntry) => {
          const entry = Option.getOrUndefined(decodePiSnapshotEntry(rawEntry));
          return entry ? [[entry.id, { entry, rawEntry }] as const] : [];
        }),
      );
      const activeEntries: Array<unknown> = [];
      const visited = new Set<string>();
      let currentEntryId = data.leafId;
      while (currentEntryId !== null && !visited.has(currentEntryId)) {
        visited.add(currentEntryId);
        const current = entriesById.get(currentEntryId);
        if (!current) break;
        activeEntries.push(current.rawEntry);
        currentEntryId = current.entry.parentId;
      }
      activeEntries.reverse();

      const turns: Array<{ id: TurnId; items: Array<unknown> }> = [];
      let activeTurn: (typeof turns)[number] | undefined;
      for (const rawEntry of activeEntries) {
        const entry = Option.getOrUndefined(decodePiSnapshotMessageEntry(rawEntry));
        if (!entry) continue;
        if (entry.message.role === "user") {
          activeTurn = { id: TurnId.make(entry.id), items: [] };
          turns.push(activeTurn);
          continue;
        }
        if (!activeTurn) {
          activeTurn = { id: TurnId.make(entry.id), items: [] };
          turns.push(activeTurn);
        }
        activeTurn.items.push(entry.message);
      }
      return { threadId, turns: turns.filter((turn) => turn.items.length > 0) };
    },
  );

  const rollbackThread: PiAdapterShape["rollbackThread"] = Effect.fn("PiAdapter.rollbackThread")(
    function* (threadId, numTurns) {
      yield* requireSession(threadId);
      if (!Number.isInteger(numTurns) || numTurns < 1) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "numTurns must be an integer >= 1.",
        });
      }
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "thread/rollback",
        detail: "Pi Agent sessions do not support provider-side rollback.",
      });
    },
  );

  const stopSession: PiAdapterShape["stopSession"] = Effect.fn("PiAdapter.stopSession")(
    function* (threadId) {
      const context = sessions.get(threadId);
      if (!context) return;
      yield* stopContext(context);
    },
  );

  const hasSession: PiAdapterShape["hasSession"] = Effect.fn("PiAdapter.hasSession")((threadId) =>
    Effect.sync(() => {
      const context = sessions.get(threadId);
      return context !== undefined && !context.stopped;
    }),
  );

  const stopAll: PiAdapterShape["stopAll"] = Effect.fn("PiAdapter.stopAll")(function* () {
    const active = [...sessions.values()];
    sessions.clear();
    yield* Effect.forEach(active, (context) => stopContext(context), {
      concurrency: "unbounded",
      discard: true,
    });
  });

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
    capabilities: { sessionModelSwitch: "in-session" },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest: (_threadId, _requestId, _decision) => unsupportedRequest("respondToRequest"),
    respondToUserInput: (_threadId, _requestId, _answers) =>
      unsupportedRequest("respondToUserInput"),
    stopSession,
    listSessions: () => Effect.succeed([...sessions.values()].map((context) => context.session)),
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    get streamEvents() {
      return Stream.fromPubSub(runtimeEvents);
    },
  } satisfies PiAdapterShape;
});
