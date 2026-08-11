import {
  PiSettings,
  type ServerProviderModel,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
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
  nonEmptyTrimmed,
  parseGenericCliVersion,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const MINIMUM_PI_VERSION = "0.84.1";
const PI_PROBE_TIMEOUT_MS = 4_000;
const PI_INVENTORY_TIMEOUT_MS = 15_000;
const PI_COMMAND_INVENTORY_TIMEOUT_MS = 10_000;
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

// Pi's `get_commands` payload. Only name and description are consumed;
// `source` (extension/prompt/skill) and `sourceInfo` (path, scope) stay native
// metadata and are stripped here.
const PiSlashCommand = Schema.Struct({
  name: Schema.String,
  description: Schema.optional(Schema.String),
});
const PiCommandInventory = Schema.Struct({
  commands: Schema.Array(PiSlashCommand),
});
const decodePiCommandInventory = Schema.decodeUnknownEffect(PiCommandInventory);
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

// Best-effort command inventory. Failures (including older Pi builds without
// `get_commands`) degrade to an empty slash-command list and must never fail
// the provider status check: Pi remains fully usable for ordinary chat.
const loadPiCommandInventory = (
  settings: PiSettings,
  cwd: string,
  environment: NodeJS.ProcessEnv,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const rpc = yield* makePiRpcProcess({
        command: configuredPiCommand(settings),
        args: ["--mode", "rpc", "--no-session"],
        cwd,
        environment,
      });
      const response = yield* rpc.request({ type: "get_commands" });
      const inventory = yield* decodePiCommandInventory(response.data);
      return inventory.commands;
    }),
  );

const loadPiModelInventory = (settings: PiSettings, cwd: string, environment: NodeJS.ProcessEnv) =>
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

function piSlashCommandsFromInventory(commands: ReadonlyArray<typeof PiSlashCommand.Type>) {
  const seen = new Set<string>();
  const slashCommands: Array<ServerProviderSlashCommand> = [];
  for (const command of commands) {
    const name = command.name.trim();
    if (!name) continue;
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    const description = nonEmptyTrimmed(command.description);
    slashCommands.push({
      name,
      ...(description ? { description } : {}),
    });
  }
  return slashCommands;
}

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

  const inventoryResult = yield* loadPiModelInventory(settings, cwd, environment).pipe(
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

  const commandInventoryResult = yield* loadPiCommandInventory(settings, cwd, environment).pipe(
    Effect.timeoutOption(PI_COMMAND_INVENTORY_TIMEOUT_MS),
    Effect.result,
  );
  const commandInventoryAvailable =
    Result.isSuccess(commandInventoryResult) && Option.isSome(commandInventoryResult.success);
  const slashCommands = Result.isSuccess(commandInventoryResult)
    ? Option.match(commandInventoryResult.success, {
        onNone: () => [],
        onSome: piSlashCommandsFromInventory,
      })
    : [];
  const commandInventoryNote = commandInventoryAvailable
    ? ""
    : " Slash-command inventory is unavailable.";

  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    slashCommands,
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
          ? `${models.length} model${models.length === 1 ? "" : "s"} available through Pi Agent.${commandInventoryNote}`
          : `Pi Agent is ready, but it reported no available models. Configure authentication and models in Pi itself.${commandInventoryNote}`,
    },
  });
});
