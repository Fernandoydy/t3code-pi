// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ApprovalRequestId,
  PiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import { attachmentRelativePath } from "../../attachmentStore.ts";
import * as ServerConfig from "../../config.ts";
import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import { makeFakePiExecutable } from "../testUtils/piFakeExecutable.ts";
import { makePiAdapter, PiResumeCursor } from "./PiAdapter.ts";

const PROVIDER = ProviderDriverKind.make("piAgent");
const INSTANCE_ID = ProviderInstanceId.make("pi_test");
const decodePiSettings = Schema.decodeSync(PiSettings);
const decodePiResumeCursor = Schema.decodeUnknownEffect(PiResumeCursor);
const NativeEventLogEntry = Schema.Struct({
  event: Schema.Struct({
    payload: Schema.StructWithRest(Schema.Struct({ type: Schema.String }), [
      Schema.Record(Schema.String, Schema.Unknown),
    ]),
  }),
});
const decodeNativeEventLogEntry = Schema.decodeUnknownOption(NativeEventLogEntry);
const encodeUnknownJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

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

describe("PiAdapter extension UI", () => {
  it.effect("maps select, confirm, input, and editor dialogs through user-input events", () => {
    const fake = makeFakePiExecutable("t3-pi-extension-ui-");
    const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
    const cases = [
      { method: "select", answer: "Green" },
      { method: "confirm", answer: "Yes" },
      { method: "input", answer: "Fernando" },
      { method: "editor", answer: "Edited text" },
    ] as const;

    return Effect.scoped(
      Effect.gen(function* () {
        for (const [index, extensionCase] of cases.entries()) {
          const threadId = ThreadId.make(`thread-pi-extension-${extensionCase.method}`);
          const adapter = yield* makePiAdapter(settings, {
            instanceId: INSTANCE_ID,
            environment: {
              ...process.env,
              PI_FAKE_EXTENSION_METHOD: extensionCase.method,
            },
          });
          const requestedFiber = yield* adapter.streamEvents.pipe(
            Stream.filter(
              (event): event is Extract<ProviderRuntimeEvent, { type: "user-input.requested" }> =>
                event.threadId === threadId && event.type === "user-input.requested",
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          const resolvedFiber = yield* adapter.streamEvents.pipe(
            Stream.filter(
              (event): event is Extract<ProviderRuntimeEvent, { type: "user-input.resolved" }> =>
                event.threadId === threadId && event.type === "user-input.resolved",
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          const terminalFiber = yield* adapter.streamEvents.pipe(
            Stream.filter(
              (
                event,
              ): event is Extract<
                ProviderRuntimeEvent,
                { type: "turn.completed" | "turn.aborted" }
              > =>
                event.threadId === threadId &&
                (event.type === "turn.completed" || event.type === "turn.aborted"),
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );

          yield* adapter.startSession(startInput(threadId));
          yield* adapter.sendTurn({ threadId, input: `extension ${String(index)}` });
          const requested = yield* Fiber.join(requestedFiber);
          expect(Option.isSome(requested)).toBe(true);
          if (Option.isNone(requested)) continue;
          expect(requested.value.requestId).toBe("fake-extension-1");
          expect(requested.value.payload.questions).toHaveLength(1);
          if (extensionCase.method === "select") {
            expect(requested.value.payload.questions[0]?.options).toHaveLength(3);
          } else if (extensionCase.method === "confirm") {
            expect(
              requested.value.payload.questions[0]?.options.map((option) => option.label),
            ).toEqual(["Yes", "No"]);
          } else {
            expect(requested.value.payload.questions[0]?.options).toEqual([]);
          }

          yield* adapter.respondToUserInput(
            threadId,
            ApprovalRequestId.make(String(requested.value.requestId)),
            { [String(requested.value.payload.questions[0]?.id)]: extensionCase.answer },
          );
          const resolved = yield* Fiber.join(resolvedFiber);
          expect(Option.isSome(resolved)).toBe(true);
          if (Option.isNone(resolved)) continue;
          expect(resolved.value.payload.answers).toEqual({
            [String(requested.value.payload.questions[0]?.id)]: extensionCase.answer,
          });
          expect(Option.isSome(yield* Fiber.join(terminalFiber))).toBe(true);
        }
      }),
    ).pipe(
      Effect.provide(testLayer()),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("rejects select answers that were not among the offered options", () => {
    const fake = makeFakePiExecutable("t3-pi-extension-select-invalid-");
    const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
    const threadId = ThreadId.make("thread-pi-extension-select-invalid");

    return Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* makePiAdapter(settings, {
          instanceId: INSTANCE_ID,
          environment: { ...process.env, PI_FAKE_EXTENSION_METHOD: "select" },
        });
        const requestedFiber = yield* adapter.streamEvents.pipe(
          Stream.filter(
            (event): event is Extract<ProviderRuntimeEvent, { type: "user-input.requested" }> =>
              event.threadId === threadId && event.type === "user-input.requested",
          ),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );

        yield* adapter.startSession(startInput(threadId));
        yield* adapter.sendTurn({ threadId, input: "pick" });
        const requested = yield* Fiber.join(requestedFiber);
        expect(Option.isSome(requested)).toBe(true);
        if (Option.isNone(requested)) return;

        const invalid = yield* adapter
          .respondToUserInput(threadId, ApprovalRequestId.make(String(requested.value.requestId)), {
            [String(requested.value.payload.questions[0]?.id)]: "Purple",
          })
          .pipe(Effect.flip);
        expect(invalid).toMatchObject({ _tag: "ProviderAdapterValidationError" });
        expect(invalid.message).toContain("offered options");

        const valid = yield* adapter.respondToUserInput(
          threadId,
          ApprovalRequestId.make(String(requested.value.requestId)),
          { [String(requested.value.payload.questions[0]?.id)]: "Green" },
        );
        expect(valid).toBeUndefined();
      }),
    ).pipe(
      Effect.provide(testLayer()),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("cancels pending dialogs on interrupt and distinguishes duplicate responses", () => {
    const fake = makeFakePiExecutable("t3-pi-extension-cancel-");
    const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
    const threadId = ThreadId.make("thread-pi-extension-cancel");

    return Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* makePiAdapter(settings, {
          instanceId: INSTANCE_ID,
          environment: { ...process.env, PI_FAKE_EXTENSION_METHOD: "input" },
        });
        const requestedFiber = yield* adapter.streamEvents.pipe(
          Stream.filter(
            (event): event is Extract<ProviderRuntimeEvent, { type: "user-input.requested" }> =>
              event.threadId === threadId && event.type === "user-input.requested",
          ),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );
        const resolvedFiber = yield* adapter.streamEvents.pipe(
          Stream.filter(
            (event): event is Extract<ProviderRuntimeEvent, { type: "user-input.resolved" }> =>
              event.threadId === threadId && event.type === "user-input.resolved",
          ),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );
        const terminalFiber = yield* adapter.streamEvents.pipe(
          Stream.filter(
            (event) =>
              event.threadId === threadId &&
              (event.type === "turn.completed" || event.type === "turn.aborted"),
          ),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );

        yield* adapter.startSession(startInput(threadId));
        yield* adapter.sendTurn({ threadId, input: "wait for input" });
        const requested = yield* Fiber.join(requestedFiber);
        expect(Option.isSome(requested)).toBe(true);
        if (Option.isNone(requested)) return;
        const requestId = ApprovalRequestId.make(String(requested.value.requestId));

        yield* adapter.interruptTurn(threadId);
        const resolved = yield* Fiber.join(resolvedFiber);
        expect(Option.isSome(resolved)).toBe(true);
        if (Option.isNone(resolved)) return;
        expect(resolved.value.payload.answers).toEqual({});
        expect(Option.isSome(yield* Fiber.join(terminalFiber))).toBe(true);

        const duplicate = yield* adapter
          .respondToUserInput(threadId, requestId, {
            [String(requested.value.payload.questions[0]?.id)]: "late",
          })
          .pipe(Effect.flip);
        expect(duplicate).toMatchObject({ _tag: "ProviderAdapterRequestError" });
        expect(duplicate.message).toContain("Unknown pending user-input request");

        const unknown = yield* adapter
          .respondToUserInput(threadId, ApprovalRequestId.make("unknown-extension-request"), {})
          .pipe(Effect.flip);
        expect(unknown).toMatchObject({ _tag: "ProviderAdapterRequestError" });
        expect(unknown.message).toContain("Unknown pending user-input request");
      }),
    ).pipe(
      Effect.provide(testLayer()),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("clears a dialog when Pi resolves its native timeout", () => {
    const fake = makeFakePiExecutable("t3-pi-extension-timeout-");
    const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
    const threadId = ThreadId.make("thread-pi-extension-timeout");

    return Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* makePiAdapter(settings, {
          instanceId: INSTANCE_ID,
          environment: {
            ...process.env,
            PI_FAKE_EXTENSION_METHOD: "input",
            PI_FAKE_EXTENSION_TIMEOUT: "10",
          },
        });
        const requestedFiber = yield* adapter.streamEvents.pipe(
          Stream.filter(
            (event): event is Extract<ProviderRuntimeEvent, { type: "user-input.requested" }> =>
              event.threadId === threadId && event.type === "user-input.requested",
          ),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );
        const resolvedFiber = yield* adapter.streamEvents.pipe(
          Stream.filter(
            (event): event is Extract<ProviderRuntimeEvent, { type: "user-input.resolved" }> =>
              event.threadId === threadId && event.type === "user-input.resolved",
          ),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );
        const terminalFiber = yield* adapter.streamEvents.pipe(
          Stream.filter(
            (event) =>
              event.threadId === threadId &&
              (event.type === "turn.completed" || event.type === "turn.aborted"),
          ),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );

        yield* adapter.startSession(startInput(threadId));
        yield* adapter.sendTurn({ threadId, input: "native timeout" });
        expect(Option.isSome(yield* Fiber.join(requestedFiber))).toBe(true);
        const resolved = yield* Fiber.join(resolvedFiber);
        expect(Option.isSome(resolved)).toBe(true);
        if (Option.isNone(resolved)) return;
        expect(resolved.value.payload.answers).toEqual({});
        const terminal = yield* Fiber.join(terminalFiber);
        expect(Option.isSome(terminal)).toBe(true);
        if (Option.isSome(terminal)) expect(terminal.value.type).toBe("turn.completed");
      }),
    ).pipe(
      Effect.provide(testLayer()),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("publishes warning and error extension notifications as runtime warnings", () => {
    const fake = makeFakePiExecutable("t3-pi-extension-notify-");
    const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
    const cases = ["notify-warning", "notify-error"] as const;

    return Effect.scoped(
      Effect.gen(function* () {
        for (const method of cases) {
          const threadId = ThreadId.make(`thread-pi-${method}`);
          const adapter = yield* makePiAdapter(settings, {
            instanceId: INSTANCE_ID,
            environment: { ...process.env, PI_FAKE_EXTENSION_METHOD: method },
          });
          const warningFiber = yield* adapter.streamEvents.pipe(
            Stream.filter(
              (event): event is Extract<ProviderRuntimeEvent, { type: "runtime.warning" }> =>
                event.threadId === threadId && event.type === "runtime.warning",
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          const terminalFiber = yield* adapter.streamEvents.pipe(
            Stream.filter(
              (event) =>
                event.threadId === threadId &&
                (event.type === "turn.completed" || event.type === "turn.aborted"),
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );

          yield* adapter.startSession(startInput(threadId));
          yield* adapter.sendTurn({ threadId, input: method });
          const warning = yield* Fiber.join(warningFiber);
          expect(Option.isSome(warning)).toBe(true);
          if (Option.isNone(warning)) continue;
          expect(warning.value.payload.message).toBe(`fake ${method}`);
          expect(Option.isSome(yield* Fiber.join(terminalFiber))).toBe(true);
        }
      }),
    ).pipe(
      Effect.provide(testLayer()),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });
});

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

  it.effect("encodes multiple image attachments in a new Pi prompt", () => {
    const fake = makeFakePiExecutable("t3-pi-adapter-images-");
    const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
    const threadId = ThreadId.make("thread-pi-images");
    const nativeEntries: Array<unknown> = [];
    const nativeEventLogger = {
      filePath: "memory://pi-native-events",
      write: (entry) => Effect.sync(() => nativeEntries.push(entry)),
      close: () => Effect.void,
    } satisfies EventNdjsonLogger;

    return Effect.scoped(
      Effect.gen(function* () {
        const { attachmentsDir } = yield* ServerConfig.ServerConfig;
        const attachments = [
          {
            type: "image" as const,
            id: "thread-pi-images-00000000-0000-4000-8000-000000000001",
            name: "diagram.png",
            mimeType: "image/png",
            sizeBytes: 4,
          },
          {
            type: "image" as const,
            id: "thread-pi-images-00000000-0000-4000-8000-000000000002",
            name: "photo.jpg",
            mimeType: "image/jpeg",
            sizeBytes: 3,
          },
        ] as const;
        for (const [attachment, bytes] of [
          [attachments[0], Uint8Array.from([0, 1, 2, 3])],
          [attachments[1], Uint8Array.from([250, 251, 252])],
        ] as const) {
          NodeFS.writeFileSync(
            NodePath.join(attachmentsDir, attachmentRelativePath(attachment)),
            bytes,
          );
        }

        const adapter = yield* makePiAdapter(settings, {
          instanceId: INSTANCE_ID,
          environment: { ...process.env, PI_FAKE_CAPTURE_IMAGES: "1" },
          nativeEventLogger,
        });
        yield* adapter.startSession(startInput(threadId));
        const eventsFiber = yield* adapter.streamEvents.pipe(
          Stream.filter((event) => event.threadId === threadId),
          Stream.takeUntil(
            (event) => event.type === "turn.completed" || event.type === "turn.aborted",
          ),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );

        const turn = yield* adapter.sendTurn({
          threadId,
          input: "What do these images show?",
          attachments,
        });
        const runtimeEvents = Array.from(yield* Fiber.join(eventsFiber));

        const prompt = nativeEntries
          .map((entry) => Option.getOrUndefined(decodeNativeEventLogEntry(entry)))
          .flatMap((entry) => (entry ? [entry.event.payload] : []))
          .find((payload) => payload.type === "test_prompt_received");
        expect(prompt).toMatchObject({
          type: "test_prompt_received",
          message: "What do these images show?",
        });
        expect(prompt?.images).toEqual([
          { type: "image", data: "AAECAw==", mimeType: "image/png" },
          { type: "image", data: "+vv8", mimeType: "image/jpeg" },
        ]);
        expect(encodeUnknownJson(runtimeEvents)).not.toContain("AAECAw==");
        expect(encodeUnknownJson(runtimeEvents)).not.toContain("+vv8");
        expect(turn.turnId).toBeTruthy();

        const imageOnlyEventsFiber = yield* adapter.streamEvents.pipe(
          Stream.filter((event) => event.threadId === threadId),
          Stream.takeUntil(
            (event) => event.type === "turn.completed" || event.type === "turn.aborted",
          ),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
        const imageOnlyTurn = yield* adapter.sendTurn({ threadId, attachments });
        const imageOnlyEvents = Array.from(yield* Fiber.join(imageOnlyEventsFiber));
        const imageOnlyPrompt = nativeEntries
          .map((entry) => Option.getOrUndefined(decodeNativeEventLogEntry(entry)))
          .flatMap((entry) => (entry ? [entry.event.payload] : []))
          .find((payload) => payload.type === "test_prompt_received" && payload.message === "");
        expect(imageOnlyPrompt?.images).toEqual([
          { type: "image", data: "AAECAw==", mimeType: "image/png" },
          { type: "image", data: "+vv8", mimeType: "image/jpeg" },
        ]);
        expect(encodeUnknownJson(imageOnlyEvents)).not.toContain("AAECAw==");
        expect(encodeUnknownJson(imageOnlyEvents)).not.toContain("+vv8");
        expect(imageOnlyTurn.turnId).not.toBe(turn.turnId);
      }),
    ).pipe(
      Effect.provide(testLayer()),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("encodes image attachments when steering an active Pi turn", () => {
    const fake = makeFakePiExecutable("t3-pi-adapter-steer-images-");
    const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
    const threadId = ThreadId.make("thread-pi-steer-images");
    const nativeEntries: Array<unknown> = [];
    const nativeEventLogger = {
      filePath: "memory://pi-native-events",
      write: (entry) => Effect.sync(() => nativeEntries.push(entry)),
      close: () => Effect.void,
    } satisfies EventNdjsonLogger;

    return Effect.scoped(
      Effect.gen(function* () {
        const { attachmentsDir } = yield* ServerConfig.ServerConfig;
        const attachment = {
          type: "image" as const,
          id: "thread-pi-steer-images-00000000-0000-4000-8000-000000000001",
          name: "redirect.webp",
          mimeType: "image/webp",
          sizeBytes: 2,
        };
        NodeFS.writeFileSync(
          NodePath.join(attachmentsDir, attachmentRelativePath(attachment)),
          Uint8Array.from([16, 17]),
        );

        const adapter = yield* makePiAdapter(settings, {
          instanceId: INSTANCE_ID,
          environment: {
            ...process.env,
            PI_FAKE_CAPTURE_IMAGES: "1",
            PI_FAKE_WAIT_FOR_ABORT: "1",
            PI_FAKE_SETTLE_ON_STEER: "1",
          },
          nativeEventLogger,
        });
        yield* adapter.startSession(startInput(threadId));
        const eventsFiber = yield* adapter.streamEvents.pipe(
          Stream.filter((event) => event.threadId === threadId),
          Stream.takeUntil(
            (event) => event.type === "turn.completed" || event.type === "turn.aborted",
          ),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );

        const firstTurn = yield* adapter.sendTurn({ threadId, input: "Start working" });
        const steeredTurn = yield* adapter.sendTurn({
          threadId,
          input: "Use this image instead",
          attachments: [attachment],
        });
        const runtimeEvents = Array.from(yield* Fiber.join(eventsFiber));

        const steer = nativeEntries
          .map((entry) => Option.getOrUndefined(decodeNativeEventLogEntry(entry)))
          .flatMap((entry) => (entry ? [entry.event.payload] : []))
          .find((payload) => payload.type === "test_steer_received");
        expect(steer).toMatchObject({
          type: "test_steer_received",
          message: "Use this image instead",
        });
        expect(steer?.images).toEqual([{ type: "image", data: "EBE=", mimeType: "image/webp" }]);
        expect(encodeUnknownJson(runtimeEvents)).not.toContain("EBE=");
        expect(steeredTurn.turnId).toBe(firstTurn.turnId);
      }),
    ).pipe(
      Effect.provide(testLayer()),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("reports missing image data and native prompt rejection as typed failures", () => {
    const fake = makeFakePiExecutable("t3-pi-adapter-image-errors-");
    const settings = decodePiSettings({ enabled: true, binaryPath: fake.executable });
    const missingThreadId = ThreadId.make("thread-pi-missing-image");
    const rejectedThreadId = ThreadId.make("thread-pi-rejected-image");
    const missingAttachment = {
      type: "image" as const,
      id: "thread-pi-missing-image-00000000-0000-4000-8000-000000000001",
      name: "missing.png",
      mimeType: "image/png",
      sizeBytes: 4,
    };
    const existingAttachment = {
      type: "image" as const,
      id: "thread-pi-rejected-image-00000000-0000-4000-8000-000000000001",
      name: "existing.png",
      mimeType: "image/png",
      sizeBytes: 4,
    };

    return Effect.scoped(
      Effect.gen(function* () {
        const { attachmentsDir } = yield* ServerConfig.ServerConfig;
        NodeFS.writeFileSync(
          NodePath.join(attachmentsDir, attachmentRelativePath(existingAttachment)),
          Uint8Array.from([0, 1, 2, 3]),
        );

        const adapter = yield* makePiAdapter(settings, { instanceId: INSTANCE_ID });
        yield* adapter.startSession(startInput(missingThreadId));
        const missing = yield* adapter
          .sendTurn({ threadId: missingThreadId, attachments: [missingAttachment] })
          .pipe(Effect.flip);
        expect(missing).toMatchObject({
          _tag: "ProviderAdapterRequestError",
          method: "prompt",
          detail: expect.stringContaining("Failed to read image attachment"),
        });

        const rejectingAdapter = yield* makePiAdapter(settings, {
          instanceId: ProviderInstanceId.make("pi_reject_image"),
          environment: { ...process.env, PI_FAKE_REJECT_FIRST_PROMPT: "1" },
        });
        yield* rejectingAdapter.startSession({
          ...startInput(rejectedThreadId),
          providerInstanceId: ProviderInstanceId.make("pi_reject_image"),
        });
        const rejection = yield* rejectingAdapter
          .sendTurn({
            threadId: rejectedThreadId,
            input: "Inspect this",
            attachments: [existingAttachment],
          })
          .pipe(Effect.flip);
        expect(rejection).toMatchObject({
          _tag: "ProviderAdapterRequestError",
          method: "prompt",
        });
        expect(rejection.message).toContain("rejected first fake prompt");
      }),
    ).pipe(
      Effect.provide(testLayer()),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

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
