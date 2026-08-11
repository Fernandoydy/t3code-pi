// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import {
  PiRpcCommandRejectedError,
  PiRpcProcessExitError,
  PiRpcProtocolError,
  PiRpcRequestUnresolvedError,
  makePiRpcProcess,
} from "./PiRpcProcess.ts";

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const fakePiPath = NodePath.join(__dirname, "../testFixtures/piRpcFake.mjs");

const makeFakePi = Effect.fn("test.makeFakePi")(function* () {
  return yield* makePiRpcProcess({
    command: process.execPath,
    args: [fakePiPath],
    cwd: process.cwd(),
  });
});

function isProcessRunning(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

describe("PiRpcProcess", () => {
  it.effect("correlates concurrent command responses by request id", () =>
    Effect.gen(function* () {
      const rpc = yield* makeFakePi();
      const firstReceived = yield* Stream.runHead(rpc.events).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const first = yield* rpc
        .request({ id: "pi-1", type: "test_deferred_response", value: "first" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Fiber.join(firstReceived);
      const second = yield* rpc
        .request({ type: "test_deferred_response", value: "second" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      const firstResponse = yield* Fiber.join(first);
      const secondResponse = yield* Fiber.join(second);
      expect(firstResponse.id).toBe("pi-1");
      expect(firstResponse.data).toEqual({ value: "first" });
      expect(secondResponse.id).toBe("pi-2");
      expect(secondResponse.data).toEqual({ value: "second" });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("frames stdout only on LF across arbitrary byte chunks", () =>
    Effect.gen(function* () {
      const rpc = yield* makeFakePi();
      const followup = yield* Stream.runHead(rpc.events).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const response = yield* rpc.request({ type: "test_unicode_separator" });

      expect(response.data).toEqual({ value: "before middle after" });
      expect(yield* Fiber.join(followup)).toMatchObject({
        _tag: "Some",
        value: { type: "chunk_followup" },
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("captures stderr without parsing it as protocol output", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const rpc = yield* makeFakePi().pipe(Effect.provideService(Scope.Scope, scope));
      yield* rpc.request({
        type: "test_stderr",
        message: '{"type":"agent_settled"}\nnot-json',
      });
      yield* Scope.close(scope, Exit.void);

      expect(yield* rpc.getStderr).toBe('{"type":"agent_settled"}\nnot-json');
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("bounds captured stderr diagnostics", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const rpc = yield* makeFakePi().pipe(Effect.provideService(Scope.Scope, scope));
      yield* rpc.request({
        type: "test_stderr",
        message: `${"x".repeat(70 * 1024)}diagnostic-tail`,
      });
      yield* Scope.close(scope, Exit.void);

      const stderr = yield* rpc.getStderr;
      expect(stderr).toHaveLength(64 * 1024);
      expect(stderr.endsWith("diagnostic-tail")).toBe(true);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("exercises deterministic state, inventory, prompt, steering, and abort behavior", () =>
    Effect.gen(function* () {
      const rpc = yield* makeFakePi();
      const eventsFiber = yield* rpc.events.pipe(
        Stream.take(5),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );

      expect((yield* rpc.request({ type: "get_state" })).data).toMatchObject({
        sessionId: "fake-session-1",
        isStreaming: false,
      });
      expect((yield* rpc.request({ type: "get_available_models" })).data).toMatchObject({
        models: [{ provider: "fake", id: "fake-model" }],
      });
      yield* rpc.request({ type: "prompt", message: "hello" });
      yield* rpc.request({ type: "steer", message: "redirect" });
      yield* rpc.request({ type: "abort" });

      expect(Array.from(yield* Fiber.join(eventsFiber)).map((event) => event.type)).toEqual([
        "agent_start",
        "message_update",
        "agent_settled",
        "queue_update",
        "agent_settled",
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("returns typed command rejection errors without closing the process", () =>
    Effect.gen(function* () {
      const rpc = yield* makeFakePi();
      const error = yield* rpc
        .request({ type: "test_reject", message: "not accepted" })
        .pipe(Effect.flip);

      expect(error).toBeInstanceOf(PiRpcCommandRejectedError);
      if (error._tag === "PiRpcCommandRejectedError") {
        expect(error.detail).toBe("not accepted");
      }
      expect((yield* rpc.request({ type: "get_state" })).success).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("fails malformed stdout and its unresolved request with typed errors", () =>
    Effect.gen(function* () {
      const rpc = yield* makeFakePi();
      const eventsFiber = yield* Stream.runDrain(rpc.events).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const requestError = yield* rpc.request({ type: "test_malformed_json" }).pipe(Effect.flip);
      const eventError = yield* Fiber.join(eventsFiber).pipe(Effect.flip);

      expect(requestError).toBeInstanceOf(PiRpcRequestUnresolvedError);
      if (requestError._tag === "PiRpcRequestUnresolvedError") {
        expect(requestError.reason).toBe("protocol-failure");
      }
      expect(eventError).toBeInstanceOf(PiRpcProtocolError);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("fails a mismatched command response without orphaning its request", () =>
    Effect.gen(function* () {
      const rpc = yield* makeFakePi();
      const eventsFiber = yield* Stream.runDrain(rpc.events).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const requestError = yield* rpc.request({ type: "test_command_mismatch" }).pipe(Effect.flip);
      const eventError = yield* Fiber.join(eventsFiber).pipe(Effect.flip);

      expect(requestError).toBeInstanceOf(PiRpcRequestUnresolvedError);
      if (requestError._tag === "PiRpcRequestUnresolvedError") {
        expect(requestError.reason).toBe("protocol-failure");
      }
      expect(eventError).toBeInstanceOf(PiRpcProtocolError);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("fails an oversized unterminated stdout record", () =>
    Effect.gen(function* () {
      const rpc = yield* makeFakePi();
      const requestError = yield* rpc.request({ type: "test_oversized_record" }).pipe(Effect.flip);

      expect(requestError).toBeInstanceOf(PiRpcRequestUnresolvedError);
      if (requestError._tag === "PiRpcRequestUnresolvedError") {
        expect(requestError.reason).toBe("protocol-failure");
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("fails instead of growing an unconsumed event buffer without bound", () =>
    Effect.gen(function* () {
      const rpc = yield* makeFakePi();
      const requestError = yield* rpc.request({ type: "test_event_overflow" }).pipe(Effect.flip);

      expect(requestError).toBeInstanceOf(PiRpcRequestUnresolvedError);
      if (requestError._tag === "PiRpcRequestUnresolvedError") {
        expect(requestError.reason).toBe("protocol-failure");
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("reports unexpected exit and all pending requests as typed failures", () =>
    Effect.gen(function* () {
      const rpc = yield* makeFakePi();
      const eventsFiber = yield* Stream.runDrain(rpc.events).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const unresolvedFiber = yield* rpc
        .request({ type: "test_never_respond" })
        .pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
      const exitRequestError = yield* rpc
        .request({ type: "test_exit", code: 23 })
        .pipe(Effect.flip);
      const unresolvedError = yield* Fiber.join(unresolvedFiber);
      const eventError = yield* Fiber.join(eventsFiber).pipe(Effect.flip);

      expect(exitRequestError).toBeInstanceOf(PiRpcRequestUnresolvedError);
      if (exitRequestError._tag === "PiRpcRequestUnresolvedError") {
        expect(exitRequestError.reason).toBe("process-exit");
      }
      expect(unresolvedError).toBeInstanceOf(PiRpcRequestUnresolvedError);
      if (unresolvedError._tag === "PiRpcRequestUnresolvedError") {
        expect(unresolvedError.reason).toBe("process-exit");
      }
      expect(eventError).toBeInstanceOf(PiRpcProcessExitError);
      if (eventError._tag === "PiRpcProcessExitError") {
        expect(eventError.exitCode).toBe(23);
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("scope closure resolves pending requests and terminates only its child", () =>
    Effect.gen(function* () {
      const firstScope = yield* Scope.make();
      const secondScope = yield* Scope.make();
      const first = yield* makeFakePi().pipe(Effect.provideService(Scope.Scope, firstScope));
      const second = yield* makeFakePi().pipe(Effect.provideService(Scope.Scope, secondScope));
      const firstEvents = yield* Stream.runCollect(first.events).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const pending = yield* first
        .request({ type: "test_never_respond" })
        .pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));

      expect(isProcessRunning(first.pid)).toBe(true);
      expect(isProcessRunning(second.pid)).toBe(true);
      yield* Scope.close(firstScope, Exit.void);

      const pendingError = yield* Fiber.join(pending);
      expect(Array.from(yield* Fiber.join(firstEvents))).toEqual([]);
      expect(pendingError).toBeInstanceOf(PiRpcRequestUnresolvedError);
      if (pendingError._tag === "PiRpcRequestUnresolvedError") {
        expect(pendingError.reason).toBe("scope-closed");
      }
      expect(isProcessRunning(first.pid)).toBe(false);
      expect(isProcessRunning(second.pid)).toBe(true);
      expect((yield* second.request({ type: "get_state" })).success).toBe(true);

      yield* Scope.close(secondScope, Exit.void);
      expect(isProcessRunning(second.pid)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
