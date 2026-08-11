// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { PiSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { describe, expect } from "vite-plus/test";

import { makeFakePiExecutable } from "../testUtils/piFakeExecutable.ts";
import { checkPiProviderStatus } from "./PiProvider.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

describe("Pi provider snapshot", () => {
  it.effect("reports a ready Pi installation from version and RPC model inventory", () => {
    const fake = makeFakePiExecutable("t3-pi-provider-");
    return Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({ enabled: true, binaryPath: fake.executable }),
        process.cwd(),
        {
          ...process.env,
          PI_FAKE_VERSION: "0.84.1",
        },
      );

      expect(snapshot).toMatchObject({
        displayName: "Pi Agent",
        enabled: true,
        installed: true,
        version: "0.84.1",
        status: "ready",
        auth: { status: "authenticated", type: "pi" },
        showInteractionModeToggle: false,
        showRuntimeModeToggle: false,
      });
      expect(snapshot.models).toEqual([
        expect.objectContaining({
          slug: "fake/fake-model",
          name: "Fake Model",
          subProvider: "fake",
          isCustom: false,
        }),
      ]);
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("publishes native model identities and only Pi-reported thinking levels", () => {
    const fake = makeFakePiExecutable("t3-pi-provider-");
    return Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({ enabled: true, binaryPath: fake.executable }),
        process.cwd(),
        {
          ...process.env,
          PI_FAKE_VERSION: "0.84.1",
          PI_FAKE_MODELS: "capabilities",
        },
      );

      expect(snapshot.status).toBe("ready");
      expect(snapshot.models).toHaveLength(3);
      expect(snapshot.models.filter((model) => model.name === "Shared Model")).toHaveLength(2);
      expect(snapshot.models.find((model) => model.slug === "gateway/org/model/v2")).toEqual({
        slug: "gateway/org/model/v2",
        name: "Shared Model",
        subProvider: "gateway",
        isCustom: false,
        capabilities: {
          optionDescriptors: [
            {
              id: "thinkingLevel",
              label: "Thinking",
              type: "select",
              options: [
                { id: "off", label: "Off" },
                { id: "high", label: "High" },
                { id: "max", label: "Max" },
              ],
            },
          ],
        },
      });
      expect(snapshot.models.find((model) => model.slug === "another/org/model/v2")).toEqual({
        slug: "another/org/model/v2",
        name: "Shared Model",
        subProvider: "another",
        isCustom: false,
        capabilities: {
          optionDescriptors: [
            {
              id: "thinkingLevel",
              label: "Thinking",
              type: "select",
              options: [
                { id: "minimal", label: "Minimal" },
                { id: "medium", label: "Medium" },
                { id: "xhigh", label: "Extra High" },
              ],
            },
          ],
        },
      });
      expect(snapshot.models.find((model) => model.slug === "plain/text-only")).toEqual({
        slug: "plain/text-only",
        name: "Text Only",
        subProvider: "plain",
        isCustom: false,
        capabilities: { optionDescriptors: [] },
      });
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("publishes native slash commands while keeping the T3 skills list empty", () => {
    const fake = makeFakePiExecutable("t3-pi-provider-");
    return Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({ enabled: true, binaryPath: fake.executable }),
        process.cwd(),
        {
          ...process.env,
          PI_FAKE_VERSION: "0.84.1",
        },
      );

      expect(snapshot.status).toBe("ready");
      expect(snapshot.slashCommands).toEqual([
        { name: "fix-tests", description: "Fix the failing test suite" },
        { name: "summarize", description: "Summarize the recent changes" },
        { name: "skill:web-search", description: "Search the web for current information" },
      ]);
      expect(snapshot.skills).toEqual([]);
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("keeps the provider ready when Pi reports an empty command inventory", () => {
    const fake = makeFakePiExecutable("t3-pi-provider-");
    return Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({ enabled: true, binaryPath: fake.executable }),
        process.cwd(),
        {
          ...process.env,
          PI_FAKE_VERSION: "0.84.1",
          PI_FAKE_COMMANDS: "empty",
        },
      );

      expect(snapshot.status).toBe("ready");
      expect(snapshot.slashCommands).toEqual([]);
      expect(snapshot.message).toContain("available through Pi Agent");
      expect(snapshot.message).not.toContain("unavailable");
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("degrades the command inventory without failing the provider", () => {
    const fake = makeFakePiExecutable("t3-pi-provider-");
    return Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({ enabled: true, binaryPath: fake.executable }),
        process.cwd(),
        {
          ...process.env,
          PI_FAKE_VERSION: "0.84.1",
          PI_FAKE_COMMANDS: "fail",
        },
      );

      expect(snapshot.status).toBe("ready");
      expect(snapshot.models).toHaveLength(1);
      expect(snapshot.slashCommands).toEqual([]);
      expect(snapshot.message).toContain("Slash-command inventory is unavailable.");
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("rejects Pi versions older than the supported RPC contract", () => {
    const fake = makeFakePiExecutable("t3-pi-provider-");
    return Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({ enabled: true, binaryPath: fake.executable }),
        process.cwd(),
        { ...process.env, PI_FAKE_VERSION: "0.84.0" },
      );

      expect(snapshot).toMatchObject({
        installed: true,
        version: "0.84.0",
        status: "error",
        auth: { status: "unknown" },
      });
      expect(snapshot.message).toContain("too old");
      expect(snapshot.models).toEqual([]);
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("reports an actionable warning when Pi has no configured models", () => {
    const fake = makeFakePiExecutable("t3-pi-provider-");
    return Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({ enabled: true, binaryPath: fake.executable }),
        process.cwd(),
        {
          ...process.env,
          PI_FAKE_VERSION: "0.84.1",
          PI_FAKE_MODELS: "empty",
        },
      );

      expect(snapshot).toMatchObject({
        installed: true,
        version: "0.84.1",
        status: "warning",
        auth: { status: "unknown", type: "pi" },
        models: [],
      });
      expect(snapshot.message).toContain("Configure authentication and models in Pi itself");
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true }))),
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("reports a missing configured executable as not installed", () =>
    Effect.gen(function* () {
      const missing = NodePath.join(
        NodeOS.tmpdir(),
        `missing-pi-${String(process.pid)}-not-present`,
      );
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({ enabled: true, binaryPath: missing }),
        process.cwd(),
        process.env,
      );

      expect(snapshot).toMatchObject({
        installed: false,
        version: null,
        status: "error",
        auth: { status: "unknown" },
      });
      expect(snapshot.message).toContain("not installed or not on PATH");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
