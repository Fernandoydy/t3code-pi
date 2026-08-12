// @effect-diagnostics nodeBuiltinImport:off
/**
 * Real-CLI smoke test for the Pi Agent integration.
 *
 * Runs the Pi provider stack (PiDriver -> PiAdapter -> PiRpcProcess) against the
 * user's actual `pi` executable with disposable session state:
 *
 * - `PI_CODING_AGENT_SESSION_DIR` points at a fresh temp directory, so native
 *   session files are written there and cleaned up afterwards. The Pi config
 *   directory is left untouched, so the user's real Pi authentication applies.
 * - `PI_OFFLINE=1` and `PI_SKIP_VERSION_CHECK=1` keep the run hermetic.
 *
 * The suite skips (the reason shows in the suite name) when no real Pi
 * binary is reachable or when Pi has no saved authentication, so CI and
 * unauthenticated machines stay green. To run it explicitly, make sure `pi`
 * is on PATH (or set `PI_SMOKE_PI_BINARY`), authenticate with `pi` + `/login`, then:
 *
 *   vp test run apps/server/src/provider/Layers/PiRealCliSmoke.test.ts
 *
 * Each test spends real model tokens; run it when you want to verify the
 * end-to-end integration against a real CLI, not on every push.
 */
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderInstanceConfigMap,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { FetchHttpClient } from "effect/unstable/http";
import { describe, expect } from "vite-plus/test";

import * as ServerConfig from "../../config.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import { PiDriver, PI_NPM_PACKAGE_NAME } from "../Drivers/PiDriver.ts";
import { ProviderVersionCache } from "../providerMaintenance.ts";
import { ProviderAdapterRegistryLive } from "./ProviderAdapterRegistry.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "./ProviderEventLoggers.ts";
import { ProviderInstanceRegistryLayer } from "./ProviderInstanceRegistryLive.ts";
import { makeProviderServiceLive } from "./ProviderService.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import { checkPiProviderStatus } from "./PiProvider.ts";
import { PiResumeCursor } from "./PiAdapter.ts";
import { BackgroundPolicyAlwaysRunLayer } from "../testUtils/piTestLayers.ts";
import * as ProviderService from "../Services/ProviderService.ts";

/** A task long enough that the turn is still active when we steer or abort into it. */
const LONG_PROMPT =
  "Write a detailed essay of about 800 words about the history of the internet. " +
  "Work through it section by section.";

function probePiVersion(command: string) {
  try {
    // On Windows, npm-style commands (pi -> pi.cmd) only resolve through
    // cmd.exe; the T3 spawner does the same via resolveSpawnCommand.
    const result = NodeChildProcess.spawnSync(command, ["--version"], {
      encoding: "utf8",
      timeout: 15_000,
      shell: NodePath.sep === "\\",
    });
    const output = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
    const match = /(?:^|\s)(\d+\.\d+\.\d+)/.exec(output.trim());
    if (result.status === 0 && match) {
      return match[1]!;
    }
  } catch {
    // keep looking
  }
  return null;
}

// Hoisted decode for resume cursors (see the prompt/resume smoke tests).
const decodePiResumeCursor = Schema.decodeUnknownEffect(PiResumeCursor);

function resolveRealPiExecutable() {
  if (process.env.PI_SMOKE_REAL_CLI === "0") {
    return null;
  }
  const requested = process.env.PI_SMOKE_PI_BINARY?.trim() || "pi";
  const isWindows = NodePath.sep === "\\";

  // 1. npm-style name, resolved through the platform shell. On Windows this
  // covers pi.cmd from npm/pnpm-style installs.
  const version = probePiVersion(requested);
  if (version) {
    return { binary: requested, explicitPath: resolveAbsolutePiPath(requested), version };
  }

  // 2. Explicit extension candidates on Windows when the bare name failed.
  if (isWindows && !NodePath.isAbsolute(requested)) {
    for (const candidate of [`${requested}.cmd`, `${requested}.exe`, `${requested}.bat`]) {
      const candidateVersion = probePiVersion(candidate);
      if (candidateVersion) {
        return { binary: candidate, explicitPath: candidate, version: candidateVersion };
      }
    }
  }
  return null;
}

/** Resolves the absolute path of the pi executable for the explicit-path scenario. */
function resolveAbsolutePiPath(binary: string) {
  if (NodePath.isAbsolute(binary)) {
    return binary;
  }
  if (NodePath.sep === "\\") {
    try {
      const output = NodeChildProcess.execFileSync("where", [binary], {
        encoding: "utf8",
        timeout: 10_000,
      });
      const entries = output
        .split(/\r?\n/)
        .map((entry) => entry.trim())
        .filter(Boolean);
      const line = entries.find((entry) => /\.(?:cmd|exe|bat)$/i.test(entry)) ?? entries[0];
      if (line) return line;
    } catch {
      // fall through to the relative name
    }
    return binary;
  }
  try {
    const output = NodeChildProcess.execFileSync("which", [binary], {
      encoding: "utf8",
      timeout: 10_000,
    });
    const line = output
      .split("\n")
      .map((entry) => entry.trim())
      .find(Boolean);
    if (line) return line;
  } catch {
    // fall through to the relative name
  }
  return binary;
}

function hasPiAuthentication() {
  const configDir =
    process.env.PI_CODING_AGENT_DIR?.trim() || NodePath.join(NodeOS.homedir(), ".pi", "agent");
  try {
    return NodeFS.statSync(NodePath.join(configDir, "auth.json")).size > 0;
  } catch {
    return false;
  }
}

const realPi = resolveRealPiExecutable();
const skipReason =
  realPi === null
    ? "no real pi executable found (install Pi 0.84.1+ or set PI_SMOKE_PI_BINARY)"
    : !hasPiAuthentication()
      ? "Pi has no saved authentication (run `pi` and use /login)"
      : null;

const smokeDescribe = skipReason === null ? describe : describe.skipIf(true);

function makeSmokeLayer(binaryPath: string, version: string, sessionDir: string) {
  const instanceId = ProviderInstanceId.make("pi_smoke");
  const configMap: ProviderInstanceConfigMap = {
    [instanceId]: {
      driver: ProviderDriverKind.make("piAgent"),
      displayName: "Pi Smoke",
      enabled: true,
      environment: [
        { name: "PI_CODING_AGENT_SESSION_DIR", value: sessionDir, sensitive: false },
        { name: "PI_OFFLINE", value: "1", sensitive: false },
        { name: "PI_SKIP_VERSION_CHECK", value: "1", sensitive: false },
      ],
      config: { binaryPath },
    },
  };
  const settingsLayer = ServerSettings.ServerSettingsService.layerTest({
    providerInstances: configMap,
  });
  const infrastructure = ServerConfig.layerTest(process.cwd(), {
    prefix: "pi-real-cli-smoke-test-",
  }).pipe(
    Layer.provideMerge(NodeServices.layer),
    Layer.provideMerge(FetchHttpClient.layer),
    Layer.provideMerge(BackgroundPolicyAlwaysRunLayer),
    Layer.provideMerge(settingsLayer),
    Layer.provideMerge(
      Layer.succeed(
        ProviderVersionCache,
        new Map([[PI_NPM_PACKAGE_NAME, { expiresAt: Number.MAX_SAFE_INTEGER, version }]]),
      ),
    ),
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

const SMOKE_INSTANCE_ID = ProviderInstanceId.make("pi_smoke");

function makeSmokeSessionDir() {
  return NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pi-real-cli-session-"));
}

function makeSmokeCwd() {
  return NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pi-real-cli-cwd-"));
}

function collectSessionFiles(sessionDir: string) {
  const files: string[] = [];
  const walk = (dir: string) => {
    for (const entry of NodeFS.readdirSync(dir, { withFileTypes: true })) {
      const full = NodePath.join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.isFile() && full.endsWith(".jsonl")) files.push(full);
    }
  };
  if (NodeFS.existsSync(sessionDir)) walk(sessionDir);
  return files.sort();
}

const smokeEnv = () => ({
  ...process.env,
  PI_OFFLINE: "1",
  PI_SKIP_VERSION_CHECK: "1",
});

smokeDescribe(
  `Pi Agent against the real CLI${skipReason === null ? "" : ` — skipped: ${skipReason}`}`,
  () => {
    it.effect(
      "loads the real model inventory and reports a supported version",
      () =>
        Effect.gen(function* () {
          const result = yield* checkPiProviderStatus(
            { binaryPath: realPi!.binary, enabled: true },
            process.cwd(),
            smokeEnv(),
          );
          expect(result.installed).toBe(true);
          expect(result.status).toBe("ready");
          expect(result.version).toMatch(/^\d+\.\d+\.\d+$/);
          expect(result.models.length).toBeGreaterThan(0);
          expect(result.models[0]?.slug).toMatch(/^[^/]+\/[^/]+$/);
        }).pipe(Effect.provide(NodeServices.layer)),
      120_000,
    );

    it.effect(
      "runs a prompt to agent_settled with disposable session state",
      () => {
        const sessionDir = makeSmokeSessionDir();
        const cwd = makeSmokeCwd();
        const threadId = ThreadId.make("thread-pi-real-cli-prompt");

        return Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          const eventsFiber = yield* provider.streamEvents.pipe(
            Stream.filter((event) => event.threadId === threadId),
            Stream.takeUntil((event) => event.type === "turn.completed"),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
          const session = yield* provider.startSession(threadId, {
            threadId,
            provider: ProviderDriverKind.make("piAgent"),
            providerInstanceId: SMOKE_INSTANCE_ID,
            cwd,
            runtimeMode: "full-access",
          });
          yield* provider.sendTurn({
            threadId,
            input: "Reply with exactly this token and nothing else: PI-SMOKE-TOKEN-42",
          });
          const events = Array.from(yield* Fiber.join(eventsFiber));

          expect(session.status).toBe("ready");
          expect(session.resumeCursor).toEqual({
            schemaVersion: 1,
            sessionFile: expect.stringMatching(/\.jsonl$/),
            sessionId: expect.any(String),
          });
          // The native session file must live in the disposable session dir.
          const cursor = yield* decodePiResumeCursor(session.resumeCursor);
          expect(NodePath.relative(sessionDir, cursor.sessionFile).split(/[\\/]/)[0]).not.toBe(
            "..",
          );
          expect(events.at(-1)).toMatchObject({
            type: "turn.completed",
            payload: { state: "completed" },
          });
          const assistantText = events
            .flatMap((event) =>
              event.type === "content.delta" && event.payload.streamKind === "assistant_text"
                ? [event.payload.delta]
                : [],
            )
            .join("");
          expect(assistantText.length).toBeGreaterThan(0);
          expect(
            events.some(
              (event) => event.type === "turn.completed" && event.payload.state === "failed",
            ),
          ).toBe(false);
        }).pipe(
          Effect.provide(makeSmokeLayer(realPi!.binary, realPi!.version, sessionDir)),
          Effect.ensuring(
            Effect.sync(() => {
              NodeFS.rmSync(sessionDir, { recursive: true, force: true });
              NodeFS.rmSync(cwd, { recursive: true, force: true });
            }),
          ),
        );
      },
      180000,
    );

    it.effect(
      "resumes the exact native session file after the process is stopped",
      () => {
        const sessionDir = makeSmokeSessionDir();
        const cwd = makeSmokeCwd();
        const threadId = ThreadId.make("thread-pi-real-cli-resume");

        return Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          const session = yield* provider.startSession(threadId, {
            threadId,
            provider: ProviderDriverKind.make("piAgent"),
            providerInstanceId: SMOKE_INSTANCE_ID,
            cwd,
            runtimeMode: "full-access",
          });
          const firstEventsFiber = yield* provider.streamEvents.pipe(
            Stream.filter((event) => event.threadId === threadId),
            Stream.takeUntil((event) => event.type === "turn.completed"),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* provider.sendTurn({ threadId, input: "Reply with the word: alpha" });
          yield* Fiber.join(firstEventsFiber);
          const sessionFilesBefore = collectSessionFiles(sessionDir);
          expect(sessionFilesBefore.length).toBeGreaterThan(0);
          yield* provider.stopSession({ threadId });

          const resumed = yield* provider.startSession(threadId, {
            threadId,
            provider: ProviderDriverKind.make("piAgent"),
            providerInstanceId: SMOKE_INSTANCE_ID,
            cwd,
            runtimeMode: "full-access",
            resumeCursor: session.resumeCursor,
          });
          const resumedEventsFiber = yield* provider.streamEvents.pipe(
            Stream.filter((event) => event.threadId === threadId),
            Stream.takeUntil((event) => event.type === "turn.completed"),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* provider.sendTurn({ threadId, input: "Reply with the word: beta" });
          const resumedEvents = Array.from(yield* Fiber.join(resumedEventsFiber));

          // Exact resume: same native session id and file, and no new session
          // file appears in the disposable session dir.
          expect(resumed.resumeCursor).toEqual(session.resumeCursor);
          expect(collectSessionFiles(sessionDir)).toEqual(sessionFilesBefore);
          expect(resumedEvents.at(-1)).toMatchObject({
            type: "turn.completed",
            payload: { state: "completed" },
          });
        }).pipe(
          Effect.provide(makeSmokeLayer(realPi!.binary, realPi!.version, sessionDir)),
          Effect.ensuring(
            Effect.sync(() => {
              NodeFS.rmSync(sessionDir, { recursive: true, force: true });
              NodeFS.rmSync(cwd, { recursive: true, force: true });
            }),
          ),
        );
      },
      240000,
    );

    it.effect(
      "steers an active native turn instead of starting a new one",
      () => {
        const sessionDir = makeSmokeSessionDir();
        const cwd = makeSmokeCwd();
        const threadId = ThreadId.make("thread-pi-real-cli-steer");

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
            providerInstanceId: SMOKE_INSTANCE_ID,
            cwd,
            runtimeMode: "full-access",
          });
          const firstDeltaFiber = yield* provider.streamEvents.pipe(
            Stream.filter((event) => event.threadId === threadId && event.type === "content.delta"),
            Stream.take(1),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
          const turn = yield* provider.sendTurn({ threadId, input: LONG_PROMPT });

          // Wait until the turn is demonstrably active before steering.
          const firstDelta = yield* Effect.timeoutOption(Fiber.join(firstDeltaFiber), "90 seconds");
          expect(Option.isSome(firstDelta)).toBe(true);

          const steered = yield* provider.sendTurn({
            threadId,
            input: "Stop writing immediately and reply with the single word: STOP.",
          });
          expect(steered.turnId).toBe(turn.turnId);

          const events = Array.from(yield* Fiber.join(eventsFiber));
          expect(events.filter((event) => event.type === "turn.started")).toHaveLength(1);
          expect(events.filter((event) => event.type === "turn.completed")).toHaveLength(1);
          expect(events.filter((event) => event.type === "turn.aborted")).toHaveLength(0);
          expect(events.at(-1)).toMatchObject({
            type: "turn.completed",
            payload: { state: "completed" },
          });
        }).pipe(
          Effect.provide(makeSmokeLayer(realPi!.binary, realPi!.version, sessionDir)),
          Effect.ensuring(
            Effect.sync(() => {
              NodeFS.rmSync(sessionDir, { recursive: true, force: true });
              NodeFS.rmSync(cwd, { recursive: true, force: true });
            }),
          ),
        );
      },
      300000,
    );

    it.effect(
      "aborts an active native turn and continues on a fresh turn",
      () => {
        const sessionDir = makeSmokeSessionDir();
        const cwd = makeSmokeCwd();
        const threadId = ThreadId.make("thread-pi-real-cli-abort");

        return Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          const abortedFiber = yield* provider.streamEvents.pipe(
            Stream.filter((event) => event.threadId === threadId),
            Stream.takeUntil((event) => event.type === "turn.aborted"),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* provider.startSession(threadId, {
            threadId,
            provider: ProviderDriverKind.make("piAgent"),
            providerInstanceId: SMOKE_INSTANCE_ID,
            cwd,
            runtimeMode: "full-access",
          });
          const firstDeltaFiber = yield* provider.streamEvents.pipe(
            Stream.filter((event) => event.threadId === threadId && event.type === "content.delta"),
            Stream.take(1),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
          const interrupted = yield* provider.sendTurn({ threadId, input: LONG_PROMPT });

          const firstDelta = yield* Effect.timeoutOption(Fiber.join(firstDeltaFiber), "90 seconds");
          expect(Option.isSome(firstDelta)).toBe(true);
          yield* provider.interruptTurn({ threadId });

          const abortedEvents = Array.from(yield* Fiber.join(abortedFiber));
          expect(abortedEvents.at(-1)).toMatchObject({
            type: "turn.aborted",
            turnId: interrupted.turnId,
          });

          const resumedFiber = yield* provider.streamEvents.pipe(
            Stream.filter((event) => event.threadId === threadId),
            Stream.takeUntil((event) => event.type === "turn.completed"),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
          const resumed = yield* provider.sendTurn({
            threadId,
            input: "Continue: reply with the word: omega",
          });
          expect(resumed.turnId).not.toBe(interrupted.turnId);
          const resumedEvents = Array.from(yield* Fiber.join(resumedFiber));
          expect(resumedEvents.at(-1)).toMatchObject({
            type: "turn.completed",
            payload: { state: "completed" },
          });
        }).pipe(
          Effect.provide(makeSmokeLayer(realPi!.binary, realPi!.version, sessionDir)),
          Effect.ensuring(
            Effect.sync(() => {
              NodeFS.rmSync(sessionDir, { recursive: true, force: true });
              NodeFS.rmSync(cwd, { recursive: true, force: true });
            }),
          ),
        );
      },
      300000,
    );

    it.effect(
      "loads the model inventory through an explicit executable path",
      () =>
        Effect.gen(function* () {
          const result = yield* checkPiProviderStatus(
            { binaryPath: realPi!.explicitPath, enabled: true },
            process.cwd(),
            smokeEnv(),
          );
          // On Windows this exercises pointing the provider at the resolved
          // pi.cmd (or pi.exe) file explicitly instead of the npm-style name.
          expect(result.installed).toBe(true);
          expect(result.status).toBe("ready");
          expect(result.models.length).toBeGreaterThan(0);
        }).pipe(Effect.provide(NodeServices.layer)),
      120_000,
    );
  },
);
