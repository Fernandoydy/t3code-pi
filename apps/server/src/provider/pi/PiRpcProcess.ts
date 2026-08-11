import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Take from "effect/Take";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

const MAX_BUFFERED_EVENTS = 4096;
const MAX_CAPTURED_STDERR_CHARS = 64 * 1024;
const MAX_STDOUT_RECORD_CHARS = 8 * 1024 * 1024;

const PiRpcWireMessageSchema = Schema.StructWithRest(Schema.Struct({ type: Schema.String }), [
  Schema.Record(Schema.String, Schema.Unknown),
]);

const PiRpcCommandSchema = Schema.StructWithRest(
  Schema.Struct({
    type: Schema.String,
    id: Schema.optional(Schema.String),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);

const PiRpcSuccessResponseSchema = Schema.StructWithRest(
  Schema.Struct({
    id: Schema.String,
    type: Schema.Literal("response"),
    command: Schema.String,
    success: Schema.Literal(true),
    data: Schema.optional(Schema.Unknown),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);

const PiRpcRejectedResponseSchema = Schema.StructWithRest(
  Schema.Struct({
    id: Schema.String,
    type: Schema.Literal("response"),
    command: Schema.String,
    success: Schema.Literal(false),
    error: Schema.String,
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);

const PiRpcResponseSchema = Schema.Union([PiRpcSuccessResponseSchema, PiRpcRejectedResponseSchema]);

const decodeWireLine = Schema.decodeUnknownEffect(Schema.fromJsonString(PiRpcWireMessageSchema));
const decodeResponse = Schema.decodeUnknownEffect(PiRpcResponseSchema);
const encodeCommand = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

export type PiRpcWireMessage = typeof PiRpcWireMessageSchema.Type;
export type PiRpcCommand = typeof PiRpcCommandSchema.Type;
export type PiRpcSuccessResponse = typeof PiRpcSuccessResponseSchema.Type;

export interface PiRpcProcessOptions {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
}

export class PiRpcSpawnError extends Schema.TaggedErrorClass<PiRpcSpawnError>()("PiRpcSpawnError", {
  command: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message() {
    return `Failed to spawn Pi RPC process: ${this.command}`;
  }
}

export class PiRpcProtocolError extends Schema.TaggedErrorClass<PiRpcProtocolError>()(
  "PiRpcProtocolError",
  {
    detail: Schema.String,
    line: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return `Invalid Pi RPC protocol output: ${this.detail}`;
  }
}

export class PiRpcProcessExitError extends Schema.TaggedErrorClass<PiRpcProcessExitError>()(
  "PiRpcProcessExitError",
  {
    exitCode: Schema.NullOr(Schema.Number),
    stderr: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return this.exitCode === null
      ? "Pi RPC process exited unexpectedly after receiving a signal"
      : `Pi RPC process exited unexpectedly with code ${String(this.exitCode)}`;
  }
}

export class PiRpcStreamError extends Schema.TaggedErrorClass<PiRpcStreamError>()(
  "PiRpcStreamError",
  {
    stream: Schema.Literals(["stdout", "stderr"]),
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return `Failed to read Pi RPC ${this.stream}`;
  }
}

export class PiRpcStdinError extends Schema.TaggedErrorClass<PiRpcStdinError>()("PiRpcStdinError", {
  command: Schema.String,
  requestId: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message() {
    return `Failed to write Pi RPC command '${this.command}' (${this.requestId})`;
  }
}

export class PiRpcCommandEncodingError extends Schema.TaggedErrorClass<PiRpcCommandEncodingError>()(
  "PiRpcCommandEncodingError",
  {
    command: Schema.String,
    requestId: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return `Failed to encode Pi RPC command '${this.command}' (${this.requestId})`;
  }
}

export class PiRpcCommandRejectedError extends Schema.TaggedErrorClass<PiRpcCommandRejectedError>()(
  "PiRpcCommandRejectedError",
  {
    command: Schema.String,
    requestId: Schema.String,
    detail: Schema.String,
  },
) {
  override get message() {
    return `Pi rejected RPC command '${this.command}' (${this.requestId}): ${this.detail}`;
  }
}

export class PiRpcDuplicateRequestIdError extends Schema.TaggedErrorClass<PiRpcDuplicateRequestIdError>()(
  "PiRpcDuplicateRequestIdError",
  {
    command: Schema.String,
    requestId: Schema.String,
  },
) {
  override get message() {
    return `Duplicate Pi RPC request id '${this.requestId}' for command '${this.command}'`;
  }
}

export class PiRpcRequestUnresolvedError extends Schema.TaggedErrorClass<PiRpcRequestUnresolvedError>()(
  "PiRpcRequestUnresolvedError",
  {
    command: Schema.String,
    requestId: Schema.String,
    reason: Schema.Literals(["process-exit", "protocol-failure", "stream-failure", "scope-closed"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return `Pi RPC request '${this.command}' (${this.requestId}) was unresolved: ${this.reason}`;
  }
}

export type PiRpcTerminalError =
  | PiRpcProtocolError
  | PiRpcProcessExitError
  | PiRpcStreamError
  | PiRpcStdinError;

export type PiRpcRequestError =
  | PiRpcCommandEncodingError
  | PiRpcCommandRejectedError
  | PiRpcDuplicateRequestIdError
  | PiRpcRequestUnresolvedError;

interface PendingRequest {
  readonly command: string;
  readonly requestId: string;
  readonly response: Deferred.Deferred<PiRpcSuccessResponse, PiRpcRequestError>;
}

type ConnectionState =
  | { readonly _tag: "Open" }
  | { readonly _tag: "Closing" }
  | { readonly _tag: "Failed"; readonly error: PiRpcTerminalError };

interface ConnectionContext {
  readonly state: ConnectionState;
  readonly pending: ReadonlyMap<string, PendingRequest>;
  readonly nextRequestId: number;
}

interface WriteRequest {
  readonly command: string;
  readonly requestId: string;
  readonly line: string;
}

type RequestRegistration =
  | { readonly _tag: "Registered"; readonly pending: PendingRequest }
  | {
      readonly _tag: "Rejected";
      readonly error: PiRpcDuplicateRequestIdError | PiRpcRequestUnresolvedError;
    };

type ResponseCorrelation =
  | { readonly _tag: "Matched"; readonly pending: PendingRequest }
  | { readonly _tag: "CommandMismatch"; readonly pending: PendingRequest }
  | { readonly _tag: "Unknown" };

function unresolvedReason(error: PiRpcTerminalError) {
  switch (error._tag) {
    case "PiRpcProcessExitError":
      return "process-exit" as const;
    case "PiRpcProtocolError":
      return "protocol-failure" as const;
    case "PiRpcStreamError":
    case "PiRpcStdinError":
      return "stream-failure" as const;
  }
}

function makeUnresolvedError(pending: PendingRequest, state: ConnectionState) {
  if (state._tag === "Failed") {
    return new PiRpcRequestUnresolvedError({
      command: pending.command,
      requestId: pending.requestId,
      reason: unresolvedReason(state.error),
      cause: state.error,
    });
  }
  return new PiRpcRequestUnresolvedError({
    command: pending.command,
    requestId: pending.requestId,
    reason: "scope-closed",
  });
}

export const makePiRpcProcess = Effect.fn("PiRpcProcess.make")(function* (
  options: PiRpcProcessOptions,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const owningScope = yield* Scope.Scope;
  const environmentOptions =
    options.environment === undefined ? {} : { env: options.environment, extendEnv: true };
  const spawnCommand = yield* resolveSpawnCommand(
    options.command,
    options.args,
    environmentOptions,
  );
  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd: options.cwd,
        ...environmentOptions,
        forceKillAfter: "1 second",
        shell: spawnCommand.shell,
        stdin: { stream: "pipe", endOnDone: false },
        stdout: "pipe",
        stderr: "pipe",
      }),
    )
    .pipe(
      Effect.provideService(Scope.Scope, owningScope),
      Effect.mapError(
        (cause) =>
          new PiRpcSpawnError({
            command: [options.command, ...options.args].join(" "),
            cause,
          }),
      ),
    );

  const contextRef = yield* Ref.make<ConnectionContext>({
    state: { _tag: "Open" },
    pending: new Map(),
    nextRequestId: 0,
  });
  const stderrRef = yield* Ref.make("");
  const writeQueue = yield* Queue.unbounded<WriteRequest>();
  const eventQueue =
    yield* Queue.sliding<Take.Take<PiRpcWireMessage, PiRpcTerminalError>>(MAX_BUFFERED_EVENTS);

  const failPending = Effect.fn("PiRpcProcess.failPending")(function* (
    pending: ReadonlyArray<PendingRequest>,
    state: ConnectionState,
  ) {
    yield* Effect.forEach(
      pending,
      (request) => Deferred.fail(request.response, makeUnresolvedError(request, state)),
      { discard: true },
    );
  });

  const failConnection = Effect.fn("PiRpcProcess.failConnection")(function* (
    error: PiRpcTerminalError,
  ) {
    const transitioned = yield* Ref.modify(contextRef, (context) => {
      if (context.state._tag !== "Open") {
        return [undefined, context] as const;
      }
      const state = { _tag: "Failed", error } as const;
      return [
        { pending: Array.from(context.pending.values()), state },
        { ...context, state, pending: new Map() },
      ] as const;
    });
    if (transitioned === undefined) return;

    yield* failPending(transitioned.pending, transitioned.state);
    yield* Queue.offer(eventQueue, Exit.fail(error));
    yield* child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
  });

  const offerEvent = Effect.fn("PiRpcProcess.offerEvent")(function* (message: PiRpcWireMessage) {
    if ((yield* Queue.size(eventQueue)) >= MAX_BUFFERED_EVENTS) {
      return yield* failConnection(
        new PiRpcProtocolError({
          detail: `event buffer exceeded ${String(MAX_BUFFERED_EVENTS)} entries`,
        }),
      );
    }
    yield* Queue.offer(eventQueue, [message]);
  });

  const handleResponse = Effect.fn("PiRpcProcess.handleResponse")(function* (
    message: PiRpcWireMessage,
  ) {
    const response = yield* decodeResponse(message).pipe(
      Effect.mapError(
        (cause) =>
          new PiRpcProtocolError({
            detail: "response did not match the Pi RPC response shape",
            cause,
          }),
      ),
      Effect.catch((error) => failConnection(error).pipe(Effect.as(undefined))),
    );
    if (response === undefined) return;

    const correlation = yield* Ref.modify(
      contextRef,
      (context): readonly [ResponseCorrelation, ConnectionContext] => {
        const pending = context.pending.get(response.id);
        if (pending === undefined) return [{ _tag: "Unknown" }, context];
        if (pending.command !== response.command) {
          return [{ _tag: "CommandMismatch", pending }, context];
        }
        const nextPending = new Map(context.pending);
        nextPending.delete(response.id);
        return [
          { _tag: "Matched", pending },
          { ...context, pending: nextPending },
        ];
      },
    );
    if (correlation._tag === "Unknown") {
      return yield* failConnection(
        new PiRpcProtocolError({
          detail: `response referenced unknown request id '${response.id}'`,
        }),
      );
    }
    if (correlation._tag === "CommandMismatch") {
      return yield* failConnection(
        new PiRpcProtocolError({
          detail: `response command '${response.command}' did not match '${correlation.pending.command}' for request '${response.id}'`,
        }),
      );
    }

    if (response.success) {
      yield* Deferred.succeed(correlation.pending.response, response);
    } else {
      yield* Deferred.fail(
        correlation.pending.response,
        new PiRpcCommandRejectedError({
          command: response.command,
          requestId: response.id,
          detail: response.error,
        }),
      );
    }
  });

  const handleLine = Effect.fn("PiRpcProcess.handleLine")(function* (rawLine: string) {
    const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
    const message = yield* decodeWireLine(line).pipe(
      Effect.mapError(
        (cause) =>
          new PiRpcProtocolError({
            detail: "stdout record was not a JSON object with a string type",
            line,
            cause,
          }),
      ),
      Effect.catch((error) => failConnection(error).pipe(Effect.as(undefined))),
    );
    if (message === undefined) return;

    if (message.type === "response") {
      yield* handleResponse(message);
    } else {
      yield* offerEvent(message);
    }
  });

  const stdoutFiber = yield* Effect.gen(function* () {
    let buffer = "";
    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) => {
        buffer += chunk;
        const lines: Array<string> = [];
        while (true) {
          const newlineIndex = buffer.indexOf("\n");
          if (newlineIndex === -1) break;
          lines.push(buffer.slice(0, newlineIndex));
          buffer = buffer.slice(newlineIndex + 1);
        }
        if (
          buffer.length > MAX_STDOUT_RECORD_CHARS ||
          lines.some((line) => line.length > MAX_STDOUT_RECORD_CHARS)
        ) {
          buffer = "";
          return failConnection(
            new PiRpcProtocolError({
              detail: `stdout record exceeded ${String(MAX_STDOUT_RECORD_CHARS)} characters`,
            }),
          );
        }
        return Effect.forEach(lines, handleLine, { discard: true });
      }),
    );
    if (buffer.length > 0) {
      yield* handleLine(buffer);
    }
  }).pipe(
    Effect.catch((cause) => failConnection(new PiRpcStreamError({ stream: "stdout", cause }))),
    Effect.forkIn(owningScope),
  );

  const stderrFiber = yield* child.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      Ref.update(stderrRef, (stderr) => `${stderr}${chunk}`.slice(-MAX_CAPTURED_STDERR_CHARS)),
    ),
    Effect.catch((cause) => failConnection(new PiRpcStreamError({ stream: "stderr", cause }))),
    Effect.forkIn(owningScope),
  );

  yield* Stream.fromQueue(writeQueue).pipe(
    Stream.runForEach((write) =>
      Stream.run(Stream.encodeText(Stream.make(write.line)), child.stdin).pipe(
        Effect.catch((cause) =>
          failConnection(
            new PiRpcStdinError({
              command: write.command,
              requestId: write.requestId,
              cause,
            }),
          ),
        ),
      ),
    ),
    Effect.forkIn(owningScope),
  );

  yield* Effect.gen(function* () {
    const processExit = yield* Effect.exit(child.exitCode);
    yield* Effect.all([Fiber.await(stdoutFiber), Fiber.await(stderrFiber)], { discard: true });
    const state = (yield* Ref.get(contextRef)).state;
    if (state._tag !== "Open") return;

    yield* failConnection(
      Exit.isSuccess(processExit)
        ? new PiRpcProcessExitError({
            exitCode: Number(processExit.value),
            stderr: yield* Ref.get(stderrRef),
          })
        : new PiRpcProcessExitError({
            exitCode: null,
            stderr: yield* Ref.get(stderrRef),
            cause: processExit.cause,
          }),
    );
  }).pipe(Effect.forkIn(owningScope));

  const close = Effect.gen(function* () {
    const closing = yield* Ref.modify(contextRef, (context) => {
      if (context.state._tag !== "Open") return [undefined, context] as const;
      const state = { _tag: "Closing" } as const;
      return [
        { pending: Array.from(context.pending.values()), state },
        { ...context, state, pending: new Map() },
      ] as const;
    });
    if (closing === undefined) return;

    yield* failPending(closing.pending, closing.state);
    yield* Queue.offer(eventQueue, Exit.void);
    yield* child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
  });
  yield* Scope.addFinalizer(owningScope, close);

  const removePending = Effect.fn("PiRpcProcess.removePending")(function* (
    pending: PendingRequest,
  ) {
    yield* Ref.update(contextRef, (context) => {
      if (context.pending.get(pending.requestId) !== pending) return context;
      const nextPending = new Map(context.pending);
      nextPending.delete(pending.requestId);
      return { ...context, pending: nextPending };
    });
  });

  const request = Effect.fn("PiRpcProcess.request")(function* (command: PiRpcCommand) {
    const response = yield* Deferred.make<PiRpcSuccessResponse, PiRpcRequestError>();
    const registration = yield* Ref.modify(
      contextRef,
      (context): readonly [RequestRegistration, ConnectionContext] => {
        let nextRequestId = context.nextRequestId;
        let requestId = command.id;
        if (requestId === undefined) {
          do {
            nextRequestId += 1;
            requestId = `pi-${String(nextRequestId)}`;
          } while (context.pending.has(requestId));
        }
        const pending = { command: command.type, requestId, response } satisfies PendingRequest;

        if (context.state._tag !== "Open") {
          return [
            {
              _tag: "Rejected",
              error: makeUnresolvedError(pending, context.state),
            },
            context,
          ];
        }
        if (context.pending.has(requestId)) {
          return [
            {
              _tag: "Rejected",
              error: new PiRpcDuplicateRequestIdError({ command: command.type, requestId }),
            },
            context,
          ];
        }
        const nextPending = new Map(context.pending);
        nextPending.set(requestId, pending);
        return [
          { _tag: "Registered", pending },
          { ...context, pending: nextPending, nextRequestId },
        ];
      },
    );
    if (registration._tag === "Rejected") return yield* registration.error;

    const pending = registration.pending;
    const line = yield* encodeCommand({ ...command, id: pending.requestId }).pipe(
      Effect.mapError(
        (cause) =>
          new PiRpcCommandEncodingError({
            command: command.type,
            requestId: pending.requestId,
            cause,
          }),
      ),
      Effect.map((json) => `${json}\n`),
      Effect.catch((error) => removePending(pending).pipe(Effect.andThen(Effect.fail(error)))),
    );
    yield* Queue.offer(writeQueue, {
      command: command.type,
      requestId: pending.requestId,
      line,
    });
    return yield* Deferred.await(response).pipe(Effect.interruptible);
  }, Effect.uninterruptible);

  return {
    pid: Number(child.pid),
    request,
    events: Stream.fromQueue(eventQueue).pipe(Stream.flattenTake),
    getStderr: Ref.get(stderrRef),
  };
});
