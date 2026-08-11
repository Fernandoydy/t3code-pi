// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { PiSettings, ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import * as ServerConfig from "../../config.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import { makeFakePiExecutable } from "../testUtils/piFakeExecutable.ts";
import { makePiAdapter, PiResumeCursor } from "./PiAdapter.ts";

const PROVIDER = ProviderDriverKind.make("piAgent");
const INSTANCE_ID = ProviderInstanceId.make("pi_test");
const decodePiSettings = Schema.decodeSync(PiSettings);
const decodePiResumeCursor = Schema.decodeUnknownEffect(PiResumeCursor);

function testLayer() {
  return ServerConfig.layerTest(process.cwd(), { prefix: "pi-adapter-test-" }).pipe(
    Layer.provideMerge(NodeServices.layer),
  );
}

const runTurn = Effect.fn("PiAdapter.test.runTurn")(function* (
  adapter: PiAdapterShape,
  threadId: ThreadId,
  input: string,
) {
  const terminalFiber = yield* adapter.streamEvents.pipe(
    Stream.filter(
      (event) =>
        event.threadId === threadId &&
        (event.type === "turn.completed" || event.type === "turn.aborted"),
    ),
    Stream.runHead,
    Effect.forkChild({ startImmediately: true }),
  );
  const turn = yield* adapter.sendTurn({ threadId, input });
  const terminal = yield* Fiber.join(terminalFiber);
  expect(Option.isSome(terminal)).toBe(true);
  return turn;
});

function startInput(threadId: ThreadId, resumeCursor?: unknown) {
  return {
    threadId,
    provider: PROVIDER,
    providerInstanceId: INSTANCE_ID,
    cwd: process.cwd(),
    runtimeMode: "full-access" as const,
    ...(resumeCursor === undefined ? {} : { resumeCursor }),
  };
}

describe("PiAdapter durable sessions", () => {
  it.effect("resumes the exact native session file and rebuilds its thread snapshot", () => {
    const fake = makeFakePiExecutable("t3-pi-adapter-");
    const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
    const threadId = ThreadId.make("thread-pi-resume");
    const adapterOptions = {
      instanceId: INSTANCE_ID,
      environment: { ...process.env, PI_FAKE_MULTI_MESSAGE_HISTORY: "1" },
    };

    return Effect.scoped(
      Effect.gen(function* () {
        const firstAdapter = yield* makePiAdapter(settings, adapterOptions);
        const firstSession = yield* firstAdapter.startSession(startInput(threadId));
        const firstTurn = yield* runTurn(firstAdapter, threadId, "first prompt");

        expect(firstSession.resumeCursor).toEqual({
          schemaVersion: 1,
          sessionFile: expect.stringMatching(/\.jsonl$/),
          sessionId: expect.stringMatching(/^fake-session-/),
        });
        expect(firstTurn.resumeCursor).toEqual(firstSession.resumeCursor);
        const cursor = yield* decodePiResumeCursor(firstSession.resumeCursor);

        yield* firstAdapter.stopSession(threadId);

        const restartedAdapter = yield* makePiAdapter(settings, adapterOptions);
        const mismatched = yield* restartedAdapter
          .startSession(startInput(threadId, { ...cursor, sessionId: "different-session" }))
          .pipe(Effect.flip);
        expect(mismatched).toMatchObject({ _tag: "ProviderAdapterProcessError" });

        const resumed = yield* restartedAdapter.startSession(startInput(threadId, cursor));
        expect(resumed.resumeCursor).toEqual(firstSession.resumeCursor);

        const recoveredSnapshot = yield* restartedAdapter.readThread(threadId);
        expect(recoveredSnapshot.turns).toHaveLength(1);
        expect(recoveredSnapshot.turns[0]).toMatchObject({
          items: [
            {
              role: "assistant",
              content: [{ type: "text", text: "fake:first prompt" }],
            },
            {
              role: "toolResult",
              toolCallId: "fake-history-tool",
              content: [{ type: "text", text: "tool output" }],
            },
            {
              role: "assistant",
              content: [{ type: "text", text: "after tool" }],
            },
          ],
        });

        yield* runTurn(restartedAdapter, threadId, "second prompt");
        const continuedSnapshot = yield* restartedAdapter.readThread(threadId);
        expect(continuedSnapshot.turns).toHaveLength(2);
      }),
    ).pipe(
      Effect.provide(testLayer()),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("fails explicitly for missing and corrupt recorded native sessions", () => {
    const fake = makeFakePiExecutable("t3-pi-adapter-");
    const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
    const missingPath = NodePath.resolve(fake.directory, "missing.jsonl");
    const corruptPath = NodePath.resolve(fake.directory, "corrupt.jsonl");
    NodeFS.writeFileSync(corruptPath, "not-json\n");

    return Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* makePiAdapter(settings, { instanceId: INSTANCE_ID });
        const invalidCursor = yield* adapter
          .startSession(
            startInput(ThreadId.make("thread-pi-invalid-cursor"), {
              schemaVersion: 2,
              sessionFile: "relative.jsonl",
              sessionId: "invalid-native-session",
            }),
          )
          .pipe(Effect.flip);
        expect(invalidCursor).toMatchObject({
          _tag: "ProviderAdapterValidationError",
          operation: "startSession",
        });

        const missing = yield* adapter
          .startSession(
            startInput(ThreadId.make("thread-pi-missing"), {
              schemaVersion: 1,
              sessionFile: missingPath,
              sessionId: "missing-native-session",
            }),
          )
          .pipe(Effect.flip);
        expect(missing).toMatchObject({
          _tag: "ProviderAdapterRequestError",
          method: "session/resume",
        });
        expect(missing.message).toContain("does not exist");

        const corrupt = yield* adapter
          .startSession(
            startInput(ThreadId.make("thread-pi-corrupt"), {
              schemaVersion: 1,
              sessionFile: corruptPath,
              sessionId: "corrupt-native-session",
            }),
          )
          .pipe(Effect.flip);
        expect(corrupt).toMatchObject({ _tag: "ProviderAdapterProcessError" });
        expect(corrupt.message).toContain("resume");
      }),
    ).pipe(
      Effect.provide(testLayer()),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect(
    "rejects rollback with the typed unsupported request error without changing state",
    () => {
      const fake = makeFakePiExecutable("t3-pi-adapter-");
      const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
      const threadId = ThreadId.make("thread-pi-rollback");

      return Effect.scoped(
        Effect.gen(function* () {
          const adapter = yield* makePiAdapter(settings, { instanceId: INSTANCE_ID });
          const session = yield* adapter.startSession(startInput(threadId));
          const rollback = yield* adapter.rollbackThread(threadId, 1).pipe(Effect.flip);

          expect(rollback).toMatchObject({
            _tag: "ProviderAdapterRequestError",
            provider: PROVIDER,
            method: "thread/rollback",
          });
          expect(yield* adapter.listSessions()).toEqual([
            expect.objectContaining({ threadId, resumeCursor: session.resumeCursor }),
          ]);
        }),
      ).pipe(
        Effect.provide(testLayer()),
        Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
      );
    },
  );

  it.effect("keeps stop, stop-all, and sibling process ownership isolated and idempotent", () => {
    const fake = makeFakePiExecutable("t3-pi-adapter-");
    const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
    const firstThreadId = ThreadId.make("thread-pi-stop-first");
    const secondThreadId = ThreadId.make("thread-pi-stop-second");

    return Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* makePiAdapter(settings, { instanceId: INSTANCE_ID });
        yield* adapter.startSession(startInput(firstThreadId));
        yield* adapter.startSession(startInput(secondThreadId));

        yield* adapter.stopSession(firstThreadId);
        yield* adapter.stopSession(firstThreadId);
        expect(yield* adapter.hasSession(firstThreadId)).toBe(false);
        expect(yield* adapter.hasSession(secondThreadId)).toBe(true);

        yield* runTurn(adapter, secondThreadId, "still alive");
        yield* adapter.stopAll();
        yield* adapter.stopAll();
        expect(yield* adapter.hasSession(secondThreadId)).toBe(false);
      }),
    ).pipe(
      Effect.provide(testLayer()),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });
});
