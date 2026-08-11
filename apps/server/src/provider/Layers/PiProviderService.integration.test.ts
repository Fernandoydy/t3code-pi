// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderInstanceConfigMap,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import * as ServerConfig from "../../config.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import { PiDriver } from "../Drivers/PiDriver.ts";
import { ProviderAdapterRegistryLive } from "./ProviderAdapterRegistry.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "./ProviderEventLoggers.ts";
import { ProviderInstanceRegistryLayer } from "./ProviderInstanceRegistryLive.ts";
import { makeProviderServiceLive } from "./ProviderService.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { makeFakePiExecutable } from "../testUtils/piFakeExecutable.ts";

const TEST_EPOCH = DateTime.makeUnsafe("1970-01-01T00:00:00.000Z");
const PiNativeLogEntry = Schema.Struct({
  event: Schema.Struct({
    payload: Schema.StructWithRest(Schema.Struct({ type: Schema.String }), [
      Schema.Record(Schema.String, Schema.Unknown),
    ]),
  }),
});
const decodePiNativeLogEntry = Schema.decodeUnknownOption(PiNativeLogEntry);

const BackgroundPolicyAlwaysRunLayer = Layer.mock(BackgroundPolicy.BackgroundPolicy)({
  reportClientActivity: () => Effect.void,
  removeRpcClient: () => Effect.void,
  reportHostPowerState: () => Effect.void,
  snapshot: Effect.succeed({
    hostPower: {
      source: "unknown",
      idle: "unknown",
      idleSeconds: null,
      locked: "unknown",
      suspended: false,
      onBattery: "unknown",
      lowPowerMode: "unknown",
      thermalState: "unknown",
      stale: true,
      updatedAt: TEST_EPOCH,
    },
    leases: [],
    activeForegroundLeaseCount: 0,
    activeScopeKeys: [],
    shouldRunOpportunisticWork: true,
    updatedAt: TEST_EPOCH,
  }),
  streamChanges: Stream.empty,
  hasDemand: () => Effect.succeed(true),
  shouldRunScopeWork: () => Effect.succeed(true),
  shouldRunOpportunisticWork: Effect.succeed(true),
});

function makeTestLayer(
  executable: string,
  eventLoggers: ProviderEventLoggers["Service"] = NoOpProviderEventLoggers,
) {
  const instanceId = ProviderInstanceId.make("pi_work");
  const configMap: ProviderInstanceConfigMap = {
    [instanceId]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Work",
      enabled: true,
      environment: [
        { name: "PI_FAKE_SCENARIO", value: "basic-turn", sensitive: false },
        { name: "PI_FAKE_MARKER", value: "instance-work", sensitive: false },
      ],
      config: { binaryPath: executable },
    },
    [ProviderInstanceId.make("pi_models")]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Models",
      enabled: true,
      environment: [{ name: "PI_FAKE_SCENARIO", value: "model-selection", sensitive: false }],
      config: { binaryPath: executable },
    },
    [ProviderInstanceId.make("pi_personal")]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Personal",
      enabled: true,
      environment: [
        { name: "PI_FAKE_SCENARIO", value: "basic-turn", sensitive: false },
        { name: "PI_FAKE_MARKER", value: "instance-personal", sensitive: false },
      ],
      config: { binaryPath: executable },
    },
    [ProviderInstanceId.make("pi_reject_once")]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Reject Once",
      enabled: true,
      environment: [
        { name: "PI_FAKE_SCENARIO", value: "basic-turn", sensitive: false },
        { name: "PI_FAKE_REJECT_FIRST_PROMPT", value: "1", sensitive: false },
      ],
      config: { binaryPath: executable },
    },
    [ProviderInstanceId.make("pi_error")]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Error",
      enabled: true,
      environment: [
        { name: "PI_FAKE_SCENARIO", value: "basic-turn", sensitive: false },
        { name: "PI_FAKE_TURN_RESULT", value: "error", sensitive: false },
      ],
      config: { binaryPath: executable },
    },
    [ProviderInstanceId.make("pi_steer")]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Steer",
      enabled: true,
      environment: [
        { name: "PI_FAKE_WAIT_FOR_ABORT", value: "1", sensitive: false },
        { name: "PI_FAKE_SETTLE_BEFORE_STEER_RESPONSE", value: "1", sensitive: false },
      ],
      config: { binaryPath: executable },
    },
    [ProviderInstanceId.make("pi_abort")]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Abort",
      enabled: true,
      environment: [
        { name: "PI_FAKE_SCENARIO", value: "basic-turn", sensitive: false },
        { name: "PI_FAKE_WAIT_FOR_ABORT_ONCE", value: "1", sensitive: false },
        { name: "PI_FAKE_ABORT_LATE_EVENTS_ON_NEXT_PROMPT", value: "1", sensitive: false },
      ],
      config: { binaryPath: executable },
    },
    [ProviderInstanceId.make("pi_abort_reject")]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Abort Reject",
      enabled: true,
      environment: [
        { name: "PI_FAKE_WAIT_FOR_ABORT", value: "1", sensitive: false },
        { name: "PI_FAKE_REJECT_ABORT", value: "1", sensitive: false },
        { name: "PI_FAKE_SETTLE_ON_STEER", value: "1", sensitive: false },
      ],
      config: { binaryPath: executable },
    },
    [ProviderInstanceId.make("pi_abort_exit")]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Abort Exit",
      enabled: true,
      environment: [
        { name: "PI_FAKE_WAIT_FOR_ABORT", value: "1", sensitive: false },
        { name: "PI_FAKE_EXIT_ON_ABORT", value: "1", sensitive: false },
      ],
      config: { binaryPath: executable },
    },
    [ProviderInstanceId.make("pi_abort_stop")]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Abort Stop",
      enabled: true,
      environment: [
        { name: "PI_FAKE_WAIT_FOR_ABORT", value: "1", sensitive: false },
        { name: "PI_FAKE_HOLD_ABORT", value: "1", sensitive: false },
      ],
      config: { binaryPath: executable },
    },
  };
  const settingsLayer = ServerSettings.ServerSettingsService.layerTest({
    providerInstances: configMap,
  });
  const infrastructure = ServerConfig.layerTest(process.cwd(), {
    prefix: "pi-provider-service-test-",
  }).pipe(
    Layer.provideMerge(NodeServices.layer),
    Layer.provideMerge(BackgroundPolicyAlwaysRunLayer),
    Layer.provideMerge(settingsLayer),
    Layer.provideMerge(Layer.succeed(ProviderEventLoggers, eventLoggers)),
  );
  const instanceRegistry = ProviderInstanceRegistryLayer({
    drivers: [PiDriver],
    configMap,
  }).pipe(Layer.provide(infrastructure));
  const adapterRegistry = ProviderAdapterRegistryLive.pipe(Layer.provide(instanceRegistry));
  const runtimeRepository = ProviderSessionRuntime.layer.pipe(
    Layer.provide(SqlitePersistenceMemory),
  );
  const directory = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepository));

  return makeProviderServiceLive().pipe(
    Layer.provide(adapterRegistry),
    Layer.provide(directory),
    Layer.provide(settingsLayer),
    Layer.provide(infrastructure),
    Layer.provide(AnalyticsService.AnalyticsService.layerTest),
    Layer.provide(Layer.succeed(ProviderEventLoggers, eventLoggers)),
  );
}

describe("Pi provider through ProviderService", () => {
  it.effect("generates thread titles and branch names with Pi as the only enabled provider", () => {
    const fake = makeFakePiExecutable("t3-pi-text-only-");
    const instanceId = ProviderInstanceId.make("pi_text_only");
    const configMap: ProviderInstanceConfigMap = {
      [instanceId]: {
        driver: ProviderDriverKind.make("piAgent"),
        displayName: "Pi Text Only",
        enabled: true,
        environment: [
          {
            name: "PI_FAKE_TEXT_GENERATION_OUTPUT",
            value: JSON.stringify({ title: "Pi-only metadata", branch: "pi-only-metadata" }),
            sensitive: false,
          },
        ],
        config: { binaryPath: fake.executable },
      },
    };
    const settingsLayer = ServerSettings.ServerSettingsService.layerTest({
      providerInstances: configMap,
    });
    const infrastructure = ServerConfig.layerTest(process.cwd(), {
      prefix: "pi-text-only-test-",
    }).pipe(
      Layer.provideMerge(NodeServices.layer),
      Layer.provideMerge(BackgroundPolicyAlwaysRunLayer),
      Layer.provideMerge(settingsLayer),
      Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
    );
    const textGenerationLayer = TextGeneration.layer.pipe(
      Layer.provide(
        ProviderInstanceRegistryLayer({ drivers: [PiDriver], configMap }).pipe(
          Layer.provide(infrastructure),
        ),
      ),
    );

    return Effect.gen(function* () {
      const textGeneration = yield* TextGeneration.TextGeneration;
      const title = yield* textGeneration.generateThreadTitle({
        cwd: process.cwd(),
        message: "Create a thread in a Pi-only installation",
        modelSelection: { instanceId, model: "auto" },
      });
      expect(title).toEqual({ title: "Pi-only metadata" });
      const branch = yield* textGeneration.generateBranchName({
        cwd: process.cwd(),
        message: "Create a branch in a Pi-only installation",
        modelSelection: { instanceId, model: "auto" },
      });
      expect(branch).toEqual({ branch: "pi-only-metadata" });
    }).pipe(
      Effect.provide(textGenerationLayer),
      Effect.ensuring(
        Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true, force: true })),
      ),
    );
  });

  it.effect("runs a selected native model until agent_settled and publishes canonical work", () => {
    const fake = makeFakePiExecutable("t3-pi-service-");
    const instanceId = ProviderInstanceId.make("pi_work");
    const threadId = ThreadId.make("thread-pi-basic");

    return Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const eventsFiber = yield* provider.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      const session = yield* provider.startSession(threadId, {
        threadId,
        provider: ProviderDriverKind.make("piAgent"),
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId, model: "fake/fake-model" },
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "hello",
        interactionMode: "plan",
      });
      const events = Array.from(yield* Fiber.join(eventsFiber));

      expect(session).toMatchObject({
        provider: "piAgent",
        providerInstanceId: instanceId,
        status: "ready",
        cwd: process.cwd(),
        model: "fake/fake-model",
      });
      expect(session.resumeCursor).toEqual({
        schemaVersion: 1,
        sessionFile: expect.stringMatching(/\.jsonl$/),
        sessionId: expect.stringMatching(/^fake-session-/),
      });
      expect(turn.resumeCursor).toEqual(session.resumeCursor);
      expect(events.map((event) => event.type)).toEqual([
        "session.started",
        "thread.started",
        "turn.started",
        "content.delta",
        "content.delta",
        "item.completed",
        "item.started",
        "item.updated",
        "item.completed",
        "content.delta",
        "item.completed",
        "turn.completed",
      ]);
      expect(
        events.filter((event) => event.type === "content.delta").map((event) => event.payload),
      ).toEqual([
        { streamKind: "reasoning_text", delta: "considering" },
        {
          streamKind: "assistant_text",
          delta: `fake:hello:${process.cwd()}:instance-work:--mode,rpc`,
        },
        { streamKind: "assistant_text", delta: "done" },
      ]);
      const assistantItems = events.filter(
        (event) =>
          event.type === "item.completed" && event.payload.itemType === "assistant_message",
      );
      expect(assistantItems).toHaveLength(2);
      expect(new Set(assistantItems.map((event) => event.itemId)).size).toBe(2);
      expect(
        events
          .filter(
            (event) =>
              event.type === "content.delta" ||
              event.type === "item.started" ||
              event.type === "item.updated" ||
              event.type === "item.completed" ||
              event.type === "turn.completed",
          )
          .every((event) => event.raw?.source === "pi.rpc.event"),
      ).toBe(true);
      expect(events.at(-1)).toMatchObject({
        provider: "piAgent",
        providerInstanceId: instanceId,
        threadId,
        turnId: turn.turnId,
        type: "turn.completed",
        payload: { state: "completed" },
      });
    }).pipe(
      Effect.provide(makeTestLayer(fake.executable)),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect(
    "recovers the persisted exact native session after its owned process is stopped",
    () => {
      const fake = makeFakePiExecutable("t3-pi-service-");
      const instanceId = ProviderInstanceId.make("pi_work");
      const threadId = ThreadId.make("thread-pi-persisted-resume");

      return Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const session = yield* provider.startSession(threadId, {
          threadId,
          provider: ProviderDriverKind.make("piAgent"),
          providerInstanceId: instanceId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
          modelSelection: { instanceId, model: "fake/fake-model" },
        });

        const firstEventsFiber = yield* provider.streamEvents.pipe(
          Stream.filter((event) => event.threadId === threadId),
          Stream.takeUntil((event) => event.type === "turn.completed"),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
        yield* provider.sendTurn({ threadId, input: "before stop" });
        yield* Fiber.join(firstEventsFiber);
        yield* provider.stopSession({ threadId });

        const resumedEventsFiber = yield* provider.streamEvents.pipe(
          Stream.filter((event) => event.threadId === threadId),
          Stream.takeUntil((event) => event.type === "turn.completed"),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
        const resumedTurn = yield* provider.sendTurn({ threadId, input: "after stop" });
        const resumedEvents = Array.from(yield* Fiber.join(resumedEventsFiber));

        expect(resumedTurn.resumeCursor).toEqual(session.resumeCursor);
        expect(
          resumedEvents.find(
            (event) =>
              event.type === "content.delta" && event.payload.streamKind === "assistant_text",
          ),
        ).toMatchObject({
          payload: {
            delta: expect.stringContaining(":--mode,rpc,--session,"),
          },
        });
        expect(resumedEvents.at(-1)).toMatchObject({
          type: "turn.completed",
          payload: { state: "completed" },
        });
      }).pipe(
        Effect.provide(makeTestLayer(fake.executable)),
        Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
      );
    },
  );

  it.effect("applies native model and thinking selections at start and between turns", () => {
    const fake = makeFakePiExecutable("t3-pi-service-");
    const instanceId = ProviderInstanceId.make("pi_models");
    const threadId = ThreadId.make("thread-pi-model-switch");

    return Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      expect(yield* provider.getCapabilities(instanceId)).toEqual({
        sessionModelSwitch: "in-session",
      });
      yield* provider.startSession(threadId, {
        threadId,
        provider: ProviderDriverKind.make("piAgent"),
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: {
          instanceId,
          model: "gateway/org/model/v2",
          options: [{ id: "thinkingLevel", value: "max" }],
        },
      });

      const firstEventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* provider.sendTurn({ threadId, input: "first" });
      const firstEvents = Array.from(yield* Fiber.join(firstEventsFiber));
      expect(firstEvents.find((event) => event.type === "content.delta")).toMatchObject({
        payload: {
          streamKind: "assistant_text",
          delta: "selection:gateway/org/model/v2:max:first",
        },
      });

      const secondEventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* provider.sendTurn({
        threadId,
        input: "second",
        modelSelection: {
          instanceId,
          model: "another/org/model/v2",
          options: [{ id: "thinkingLevel", value: "xhigh" }],
        },
      });
      const secondEvents = Array.from(yield* Fiber.join(secondEventsFiber));
      expect(secondEvents.find((event) => event.type === "content.delta")).toMatchObject({
        payload: {
          streamKind: "assistant_text",
          delta: "selection:another/org/model/v2:xhigh:second",
        },
      });

      const thirdEventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* provider.sendTurn({
        threadId,
        input: "third",
        modelSelection: {
          instanceId,
          model: "another/org/model/v2",
          options: [{ id: "thinkingLevel", value: "high" }],
        },
      });
      const thirdEvents = Array.from(yield* Fiber.join(thirdEventsFiber));
      expect(thirdEvents.find((event) => event.type === "content.delta")).toMatchObject({
        payload: {
          streamKind: "assistant_text",
          delta: "selection:another/org/model/v2:high:third",
        },
      });
      expect(yield* provider.listSessions()).toContainEqual(
        expect.objectContaining({
          threadId,
          model: "another/org/model/v2",
        }),
      );
    }).pipe(
      Effect.provide(makeTestLayer(fake.executable)),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("recovers after Pi rejects a prompt before accepting it", () => {
    const fake = makeFakePiExecutable("t3-pi-service-");
    const instanceId = ProviderInstanceId.make("pi_reject_once");
    const threadId = ThreadId.make("thread-pi-reject-once");

    return Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      yield* provider.startSession(threadId, {
        threadId,
        provider: ProviderDriverKind.make("piAgent"),
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId, model: "fake/fake-model" },
      });

      const failedEventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      const rejection = yield* provider
        .sendTurn({ threadId, input: "reject me" })
        .pipe(Effect.flip);
      const failedEvents = Array.from(yield* Fiber.join(failedEventsFiber));

      expect(rejection.message).toContain("rejected first fake prompt");
      expect(failedEvents.at(-1)).toMatchObject({
        type: "turn.completed",
        payload: { state: "failed" },
      });

      const completedEventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* provider.sendTurn({ threadId, input: "try again" });
      expect(Array.from(yield* Fiber.join(completedEventsFiber)).at(-1)).toMatchObject({
        type: "turn.completed",
        payload: { state: "completed" },
      });
    }).pipe(
      Effect.provide(makeTestLayer(fake.executable)),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("reports Pi model failures instead of completing the turn", () => {
    const fake = makeFakePiExecutable("t3-pi-service-");
    const instanceId = ProviderInstanceId.make("pi_error");
    const threadId = ThreadId.make("thread-pi-error");

    return Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      yield* provider.startSession(threadId, {
        threadId,
        provider: ProviderDriverKind.make("piAgent"),
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId, model: "fake/fake-model" },
      });
      const eventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );

      yield* provider.sendTurn({ threadId, input: "fail" });

      expect(Array.from(yield* Fiber.join(eventsFiber)).at(-1)).toMatchObject({
        type: "turn.completed",
        payload: { state: "failed", errorMessage: "fake model failure" },
      });
    }).pipe(
      Effect.provide(makeTestLayer(fake.executable)),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("keeps one turn when Pi settles before the steer response", () => {
    const fake = makeFakePiExecutable("t3-pi-service-");
    const instanceId = ProviderInstanceId.make("pi_steer");
    const threadId = ThreadId.make("thread-pi-steer");

    return Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const eventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* provider.startSession(threadId, {
        threadId,
        provider: ProviderDriverKind.make("piAgent"),
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId, model: "fake/fake-model" },
      });

      const first = yield* provider.sendTurn({ threadId, input: "start" });
      const steered = yield* provider.sendTurn({ threadId, input: "redirect" });
      const events = Array.from(yield* Fiber.join(eventsFiber));

      expect(steered.turnId).toBe(first.turnId);
      expect(events.filter((event) => event.type === "turn.started")).toHaveLength(1);
      expect(events.filter((event) => event.type === "turn.completed")).toHaveLength(1);
      expect(events).toContainEqual(
        expect.objectContaining({
          type: "content.delta",
          turnId: first.turnId,
          payload: { streamKind: "assistant_text", delta: "steered:redirect" },
        }),
      );
    }).pipe(
      Effect.provide(makeTestLayer(fake.executable)),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("fences delayed abort events when reusing the Pi session", () => {
    const fake = makeFakePiExecutable("t3-pi-service-");
    const instanceId = ProviderInstanceId.make("pi_abort");
    const threadId = ThreadId.make("thread-pi-abort");

    return Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const eventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* provider.startSession(threadId, {
        threadId,
        provider: ProviderDriverKind.make("piAgent"),
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId, model: "fake/fake-model" },
      });

      const interrupted = yield* provider.sendTurn({ threadId, input: "wait" });
      yield* provider.interruptTurn({ threadId });
      const resumed = yield* provider.sendTurn({ threadId, input: "continue" });
      const events = Array.from(yield* Fiber.join(eventsFiber));
      const interruptedTerminals = events.filter(
        (event) =>
          event.turnId === interrupted.turnId &&
          (event.type === "turn.aborted" || event.type === "turn.completed"),
      );

      expect(resumed.turnId).not.toBe(interrupted.turnId);
      expect(interruptedTerminals).toHaveLength(1);
      expect(interruptedTerminals[0]?.type).toBe("turn.aborted");
      expect(
        events.some(
          (event) =>
            event.type === "content.delta" && event.payload.delta === "late-aborted-content",
        ),
      ).toBe(false);
      expect(events.at(-1)).toMatchObject({
        type: "turn.completed",
        turnId: resumed.turnId,
        payload: { state: "completed" },
      });
    }).pipe(
      Effect.provide(makeTestLayer(fake.executable)),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("keeps the active Pi turn running when native abort is rejected", () => {
    const fake = makeFakePiExecutable("t3-pi-service-");
    const instanceId = ProviderInstanceId.make("pi_abort_reject");
    const threadId = ThreadId.make("thread-pi-abort-reject");

    return Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const eventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* provider.startSession(threadId, {
        threadId,
        provider: ProviderDriverKind.make("piAgent"),
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId, model: "fake/fake-model" },
      });

      const turn = yield* provider.sendTurn({ threadId, input: "wait" });
      const interruptError = yield* provider.interruptTurn({ threadId }).pipe(Effect.flip);
      const steered = yield* provider.sendTurn({ threadId, input: "keep going" });
      const events = Array.from(yield* Fiber.join(eventsFiber));

      expect(interruptError).toMatchObject({
        _tag: "ProviderAdapterRequestError",
        method: "abort",
      });
      expect(steered.turnId).toBe(turn.turnId);
      expect(events.filter((event) => event.type === "turn.aborted")).toHaveLength(0);
      expect(events.at(-1)).toMatchObject({
        type: "turn.completed",
        turnId: turn.turnId,
        payload: { state: "completed" },
      });
    }).pipe(
      Effect.provide(makeTestLayer(fake.executable)),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("fails deterministically when the Pi process exits during interruption", () => {
    const fake = makeFakePiExecutable("t3-pi-service-");
    const instanceId = ProviderInstanceId.make("pi_abort_exit");
    const threadId = ThreadId.make("thread-pi-abort-exit");

    return Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const eventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "session.exited"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* provider.startSession(threadId, {
        threadId,
        provider: ProviderDriverKind.make("piAgent"),
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId, model: "fake/fake-model" },
      });

      const turn = yield* provider.sendTurn({ threadId, input: "wait" });
      const interruptError = yield* provider.interruptTurn({ threadId }).pipe(Effect.flip);
      const events = Array.from(yield* Fiber.join(eventsFiber));
      const terminals = events.filter(
        (event) =>
          event.turnId === turn.turnId &&
          (event.type === "turn.aborted" || event.type === "turn.completed"),
      );

      expect(interruptError).toMatchObject({
        _tag: "ProviderAdapterRequestError",
        method: "abort",
      });
      expect(terminals).toHaveLength(1);
      expect(terminals[0]).toMatchObject({
        type: "turn.completed",
        payload: { state: "failed" },
      });
      expect(events.at(-1)).toMatchObject({
        type: "session.exited",
        payload: { exitKind: "error" },
      });
    }).pipe(
      Effect.provide(makeTestLayer(fake.executable)),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("lets session stop win a pending Pi abort without duplicate terminals", () => {
    const fake = makeFakePiExecutable("t3-pi-service-");
    const instanceId = ProviderInstanceId.make("pi_abort_stop");
    const threadId = ThreadId.make("thread-pi-abort-stop");
    const abortReceived = Deferred.makeUnsafe<void>();
    const eventLoggers = ProviderEventLoggers.of({
      native: {
        filePath: "memory://pi-native-events",
        write: (entry) => {
          const decoded = Option.getOrUndefined(decodePiNativeLogEntry(entry));
          return decoded?.event.payload.type === "test_abort_received"
            ? Deferred.succeed(abortReceived, undefined).pipe(Effect.asVoid)
            : Effect.void;
        },
        close: () => Effect.void,
      },
      canonical: undefined,
    });

    return Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const eventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "session.exited"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* provider.startSession(threadId, {
        threadId,
        provider: ProviderDriverKind.make("piAgent"),
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId, model: "fake/fake-model" },
      });
      const turn = yield* provider.sendTurn({ threadId, input: "wait" });
      const interruptFiber = yield* provider
        .interruptTurn({ threadId })
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(abortReceived);
      yield* provider.stopSession({ threadId });
      const interruptExit = yield* Fiber.join(interruptFiber);
      const events = Array.from(yield* Fiber.join(eventsFiber));
      const terminals = events.filter(
        (event) =>
          event.turnId === turn.turnId &&
          (event.type === "turn.aborted" || event.type === "turn.completed"),
      );

      expect(Exit.isFailure(interruptExit)).toBe(true);
      expect(terminals).toHaveLength(1);
      expect(terminals[0]?.type).toBe("turn.aborted");
      expect(events.at(-1)?.type).toBe("session.exited");
    }).pipe(
      Effect.provide(makeTestLayer(fake.executable, eventLoggers)),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });

  it.effect("routes independent Pi configurations exclusively by instance id", () => {
    const fake = makeFakePiExecutable("t3-pi-service-");
    const workId = ProviderInstanceId.make("pi_work");
    const personalId = ProviderInstanceId.make("pi_personal");
    const threadId = ThreadId.make("thread-pi-personal");

    return Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      expect(yield* provider.getInstanceInfo(workId)).toMatchObject({
        instanceId: workId,
        displayName: "Pi Work",
      });
      expect(yield* provider.getInstanceInfo(personalId)).toMatchObject({
        instanceId: personalId,
        displayName: "Pi Personal",
      });

      const eventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* provider.startSession(threadId, {
        threadId,
        provider: ProviderDriverKind.make("piAgent"),
        providerInstanceId: personalId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: personalId, model: "fake/fake-model" },
      });
      yield* provider.sendTurn({ threadId, input: "personal" });
      const events = Array.from(yield* Fiber.join(eventsFiber));
      const assistantDelta = events.find(
        (event) => event.type === "content.delta" && event.payload.streamKind === "assistant_text",
      );

      expect(assistantDelta).toMatchObject({
        providerInstanceId: personalId,
        payload: {
          delta: `fake:personal:${process.cwd()}:instance-personal:--mode,rpc`,
        },
      });
      expect(events.every((event) => event.providerInstanceId === personalId)).toBe(true);
    }).pipe(
      Effect.provide(makeTestLayer(fake.executable)),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
    );
  });
});
