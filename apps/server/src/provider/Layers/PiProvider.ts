import { PiSettings, type ServerProviderModel } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { compareSemverVersions } from "@t3tools/shared/semver";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ChildProcess } from "effect/unstable/process";
import type { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner";

import {
  PI_THINKING_LEVEL_OPTION_ID,
  PiThinkingLevel,
  piThinkingLevelLabel,
} from "../pi/PiModel.ts";
import { makePiRpcProcess } from "../pi/PiRpcProcess.ts";
import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const MINIMUM_PI_VERSION = "0.84.1";
const PI_PROBE_TIMEOUT_MS = 4_000;
const PI_INVENTORY_TIMEOUT_MS = 15_000;
const PI_PRESENTATION = {
  displayName: "Pi Agent",
  showInteractionModeToggle: false,
  showRuntimeModeToggle: false,
} as const;
const EMPTY_MODEL_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });
const PiThinkingLevels = Schema.Struct({ levels: Schema.Array(PiThinkingLevel) });
const decodePiThinkingLevels = Schema.decodeUnknownEffect(PiThinkingLevels);

const PiInventoryModel = Schema.Struct({
  provider: Schema.String,
  id: Schema.String,
  name: Schema.optional(Schema.String),
  reasoning: Schema.optional(Schema.Boolean),
});
const PiInventory = Schema.Struct({ models: Schema.Array(PiInventoryModel) });
const decodePiInventory = Schema.decodeUnknownEffect(PiInventory);
const PiModelInventory = Schema.Array(
  Schema.Struct({
    model: PiInventoryModel,
    thinkingLevels: Schema.Array(PiThinkingLevel),
  }),
);

function configuredPiCommand(settings: PiSettings) {
  return settings.binaryPath || "pi";
}

function piModelCapabilities(thinkingLevels: ReadonlyArray<typeof PiThinkingLevel.Type>) {
  if (thinkingLevels.length === 0) return EMPTY_MODEL_CAPABILITIES;
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: PI_THINKING_LEVEL_OPTION_ID,
        label: "Thinking",
        type: "select",
        options: thinkingLevels.map((level) => ({
          id: level,
          label: piThinkingLevelLabel(level),
        })),
      },
    ],
  });
}

function piModelsFromInventory(inventory: typeof PiModelInventory.Type) {
  const seen = new Set<string>();
  const models: Array<ServerProviderModel> = [];
  for (const { model: nativeModel, thinkingLevels } of inventory) {
    const provider = nativeModel.provider.trim();
    const modelId = nativeModel.id.trim();
    if (!provider || !modelId) continue;
    const slug = `${provider}/${modelId}`;
    if (seen.has(slug)) continue;
    seen.add(slug);
    models.push({
      slug,
      name: nativeModel.name?.trim() || modelId,
      subProvider: provider,
      isCustom: false,
      capabilities: piModelCapabilities(thinkingLevels),
    });
  }
  return models.toSorted((left, right) => left.name.localeCompare(right.name));
}

const runPiVersionCommand = (settings: PiSettings, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const command = configuredPiCommand(settings);
    const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
      env: environment,
      extendEnv: true,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        extendEnv: true,
        shell: spawnCommand.shell,
      }),
    );
  });

const loadPiInventory = (settings: PiSettings, cwd: string, environment: NodeJS.ProcessEnv) =>
  Effect.scoped(
    Effect.gen(function* () {
      const rpc = yield* makePiRpcProcess({
        command: configuredPiCommand(settings),
        args: ["--mode", "rpc", "--no-session"],
        cwd,
        environment,
      });
      const loadModelThinkingLevels = Effect.fn("PiProvider.loadModelThinkingLevels")(function* (
        model: typeof PiInventoryModel.Type,
      ) {
        // Pi reports only "off" here; omit a one-choice control for non-reasoning models.
        if (model.reasoning !== true) {
          return { model, thinkingLevels: [] };
        }
        yield* rpc.request({
          type: "set_model",
          provider: model.provider,
          modelId: model.id,
        });
        const levelsResponse = yield* rpc.request({
          type: "get_available_thinking_levels",
        });
        const { levels } = yield* decodePiThinkingLevels(levelsResponse.data);
        return { model, thinkingLevels: levels };
      });
      const response = yield* rpc.request({ type: "get_available_models" });
      const inventory = yield* decodePiInventory(response.data);
      return yield* Effect.forEach(inventory.models, loadModelThinkingLevels, {
        concurrency: 1,
      });
    }),
  );

export function buildInitialPiProviderSnapshot(settings: PiSettings) {
  return Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: [],
      probe: settings.enabled
        ? {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Pi Agent provider status has not been checked in this session yet.",
          }
        : {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Pi Agent is disabled in T3 Code settings.",
          },
    });
  });
}

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  settings: PiSettings,
  cwd: string,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const buildFailure = (input: {
    readonly installed: boolean;
    readonly version: string | null;
    readonly message: string;
  }) =>
    buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: [],
      probe: {
        installed: input.installed,
        version: input.version,
        status: "error",
        auth: { status: "unknown" },
        message: input.message,
      },
    });

  if (!settings.enabled) {
    return yield* buildInitialPiProviderSnapshot(settings);
  }

  const versionResult = yield* runPiVersionCommand(settings, environment).pipe(
    Effect.timeoutOption(PI_PROBE_TIMEOUT_MS),
    Effect.result,
  );
  if (Result.isFailure(versionResult)) {
    return buildFailure({
      installed: !isCommandMissingCause(versionResult.failure),
      version: null,
      message: isCommandMissingCause(versionResult.failure)
        ? "Pi Agent (`pi`) is not installed or not on PATH. Install Pi 0.84.1 or newer, or configure its binary path."
        : "Failed to execute the Pi Agent version check. Check the configured binary path.",
    });
  }
  if (Option.isNone(versionResult.success)) {
    return buildFailure({
      installed: true,
      version: null,
      message: "Pi Agent timed out while running `pi --version`.",
    });
  }

  const versionCommand = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionCommand.stdout}\n${versionCommand.stderr}`);
  if (versionCommand.code !== 0) {
    return buildFailure({
      installed: true,
      version,
      message: "Pi Agent is installed but `pi --version` failed.",
    });
  }
  if (!version) {
    return buildFailure({
      installed: true,
      version: null,
      message: `Unable to determine the Pi Agent version. T3 Code requires Pi ${MINIMUM_PI_VERSION} or newer.`,
    });
  }
  if (compareSemverVersions(version, MINIMUM_PI_VERSION) < 0) {
    return buildFailure({
      installed: true,
      version,
      message: `Pi Agent ${version} is too old. Upgrade to ${MINIMUM_PI_VERSION} or newer.`,
    });
  }

  const inventoryResult = yield* loadPiInventory(settings, cwd, environment).pipe(
    Effect.timeoutOption(PI_INVENTORY_TIMEOUT_MS),
    Effect.result,
  );
  if (Result.isFailure(inventoryResult)) {
    return buildFailure({
      installed: true,
      version,
      message:
        "Pi Agent is installed, but its RPC model inventory could not be loaded. Check Pi configuration and server logs.",
    });
  }
  if (Option.isNone(inventoryResult.success)) {
    return buildFailure({
      installed: true,
      version,
      message: "Pi Agent RPC model inventory timed out.",
    });
  }

  const models = piModelsFromInventory(inventoryResult.success.value);
  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    probe: {
      installed: true,
      version,
      status: models.length > 0 ? "ready" : "warning",
      auth:
        models.length > 0
          ? { status: "authenticated", type: "pi" }
          : { status: "unknown", type: "pi" },
      message:
        models.length > 0
          ? `${models.length} model${models.length === 1 ? "" : "s"} available through Pi Agent.`
          : "Pi Agent is ready, but it reported no available models. Configure authentication and models in Pi itself.",
    },
  });
});
