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
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
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
import { makeFakePiExecutable } from "../testUtils/piFakeExecutable.ts";

const TEST_EPOCH = DateTime.makeUnsafe("1970-01-01T00:00:00.000Z");

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

function makeTestLayer(executable: string) {
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
    [ProviderInstanceId.make("pi_abort")]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Abort",
      enabled: true,
      environment: [{ name: "PI_FAKE_WAIT_FOR_ABORT", value: "1", sensitive: false }],
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
    Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
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
    Layer.provide(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
  );
}

describe("Pi provider through ProviderService", () => {
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
      expect(session.resumeCursor).toBeUndefined();
      expect(turn.resumeCursor).toBeUndefined();
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

  it.effect("reports interrupted Pi turns as aborted", () => {
    const fake = makeFakePiExecutable("t3-pi-service-");
    const instanceId = ProviderInstanceId.make("pi_abort");
    const threadId = ThreadId.make("thread-pi-abort");

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
        Stream.takeUntil((event) => event.type === "turn.aborted"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );

      yield* provider.sendTurn({ threadId, input: "wait" });
      yield* provider.interruptTurn({ threadId });

      expect(Array.from(yield* Fiber.join(eventsFiber)).at(-1)).toMatchObject({
        type: "turn.aborted",
      });
    }).pipe(
      Effect.provide(makeTestLayer(fake.executable)),
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
