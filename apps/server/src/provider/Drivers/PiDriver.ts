import {
  PiSettings,
  ProviderDriverKind,
  TextGenerationError,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";

import type * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makePiAdapter } from "../Layers/PiAdapter.ts";
import { buildInitialPiProviderSnapshot, checkPiProviderStatus } from "../Layers/PiProvider.ts";
import { ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  makePackageManagedProviderMaintenanceResolver,
  makeProviderMaintenanceCapabilities,
  normalizeCommandPath,
  resolveProviderMaintenanceCapabilitiesEffect,
} from "../providerMaintenance.ts";
import type { ProviderMaintenanceCapabilityResolutionOptions } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);
const DRIVER_KIND = ProviderDriverKind.make("piAgent");
export const PI_NPM_PACKAGE_NAME = "@earendil-works/pi-coding-agent";
const PI_NPM_PACKAGE_PATH = `/node_modules/${PI_NPM_PACKAGE_NAME.toLowerCase()}/`;
const PI_NATIVE_COMMAND_PATH_SUFFIXES = [
  "/.local/bin/pi",
  "/.local/bin/pi.exe",
  "/.local/bin/pi.cmd",
  "/.bun/bin/pi",
  "/.bun/bin/pi.exe",
  "/.local/share/pnpm/pi",
  "/.local/share/pnpm/pi.cmd",
  "/appdata/local/pnpm/bin/pi",
  "/appdata/local/pnpm/bin/pi.cmd",
  "/appdata/roaming/npm/pi",
  "/appdata/roaming/npm/pi.cmd",
  "/.npm-global/bin/pi",
  "/.npm-global/bin/pi.cmd",
  "/.yarn/bin/pi",
  "/.yarn/bin/pi.cmd",
  "/node_modules/.bin/pi",
  "/node_modules/.bin/pi.cmd",
];

// Pi's self-updater can identify the package manager only for its own global
// installation. Keep this allowlist narrow: an explicit executable elsewhere
// gets a version advisory but must be updated manually.
export function isPiNativeCommandPath(commandPath: string) {
  const normalized = normalizeCommandPath(commandPath);
  return (
    normalized.includes(PI_NPM_PACKAGE_PATH) ||
    PI_NATIVE_COMMAND_PATH_SUFFIXES.some((suffix) => normalized.endsWith(suffix))
  );
}

const PI_NATIVE_UPDATE_ACTION = {
  executable: "pi",
  args: ["update", "--self"],
  lockKey: "pi-native",
} as const;
const PI_NATIVE_UPDATE = makeProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: PI_NPM_PACKAGE_NAME,
  updateExecutable: PI_NATIVE_UPDATE_ACTION.executable,
  updateArgs: PI_NATIVE_UPDATE_ACTION.args,
  updateLockKey: PI_NATIVE_UPDATE_ACTION.lockKey,
});
const PI_PACKAGE_UPDATE = makePackageManagedProviderMaintenanceResolver({
  provider: DRIVER_KIND,
  npmPackageName: PI_NPM_PACKAGE_NAME,
  homebrewFormula: null,
  nativeUpdate: {
    ...PI_NATIVE_UPDATE_ACTION,
    isCommandPath: isPiNativeCommandPath,
  },
});
const PI_UPDATE = {
  resolve: (options?: ProviderMaintenanceCapabilityResolutionOptions) => {
    const binaryPath = options?.binaryPath?.trim().toLowerCase();
    if (!binaryPath || binaryPath === "pi" || binaryPath === "pi.cmd" || binaryPath === "pi.exe") {
      return PI_NATIVE_UPDATE;
    }
    return PI_PACKAGE_UPDATE.resolve(options);
  },
};

export function resolvePiProviderMaintenanceCapabilities(
  options?: ProviderMaintenanceCapabilityResolutionOptions,
) {
  return PI_UPDATE.resolve(options);
}

export type PiDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | Path.Path
  | ProviderEventLoggers
  | ServerConfig
  | ServerSettingsService;

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

function unsupportedTextGeneration(): TextGeneration.TextGeneration["Service"] {
  const unsupported = (operation: string) =>
    Effect.fail(
      new TextGenerationError({
        operation,
        detail: "Pi Agent auxiliary text generation is not available yet.",
      }),
    );
  return {
    generateCommitMessage: () => unsupported("generateCommitMessage"),
    generatePrContent: () => unsupported("generatePrContent"),
    generateBranchName: () => unsupported("generateBranchName"),
    generateThreadTitle: () => unsupported("generateThreadTitle"),
  };
}

export const PiDriver: ProviderDriver<PiSettings, PiDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Pi Agent",
    supportsMultipleInstances: true,
  },
  configSchema: PiSettings,
  defaultConfig: () => decodePiSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const serverConfig = yield* ServerConfig;
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const serverSettings = yield* ServerSettingsService;
      const httpClient = yield* HttpClient.HttpClient;
      const eventLoggers = yield* ProviderEventLoggers;
      const processEnvironment = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies PiSettings;
      const maintenanceCapabilities = yield* resolveProviderMaintenanceCapabilitiesEffect(
        PI_UPDATE,
        {
          binaryPath: effectiveConfig.binaryPath,
          env: processEnvironment,
        },
      );
      const adapter = yield* makePiAdapter(effectiveConfig, {
        instanceId,
        environment: processEnvironment,
        ...(eventLoggers.native ? { nativeEventLogger: eventLoggers.native } : {}),
      });
      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<PiSettings>>({
        maintenanceCapabilities,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialPiProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider: checkPiProviderStatus(
          effectiveConfig,
          serverConfig.cwd,
          processEnvironment,
        ).pipe(
          Effect.map(stampIdentity),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
        ),
        enrichSnapshot: ({ settings, snapshot, publishSnapshot }) =>
          enrichProviderSnapshotWithVersionAdvisory(snapshot, maintenanceCapabilities, {
            enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
          }).pipe(
            Effect.provideService(HttpClient.HttpClient, httpClient),
            Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
          ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Pi Agent snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration: unsupportedTextGeneration(),
      } satisfies ProviderInstance;
    }),
};
