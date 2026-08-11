// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  DEFAULT_TEXT_GENERATION_MODEL,
  PiSettings,
  ProviderInstanceId,
  TextGenerationError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { describe, expect } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import { makeFakePiExecutable } from "../provider/testUtils/piFakeExecutable.ts";
import * as TextGeneration from "./TextGeneration.ts";
import { makePiTextGeneration } from "./PiTextGeneration.ts";

const INSTANCE_ID = ProviderInstanceId.make("pi_text");
const decodePiSettings = Schema.decodeSync(PiSettings);
const testLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-pi-text-generation-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

function withFakePi<A, E, R>(
  environment: NodeJS.ProcessEnv,
  effectFn: (input: {
    readonly textGeneration: TextGeneration.TextGeneration["Service"];
    readonly argsPath: string;
    readonly statePath: string;
    readonly fileSystem: FileSystem.FileSystem;
  }) => Effect.Effect<A, E, R>,
) {
  return Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const tempDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3code-pi-text-test-artifacts-",
    });
    const fake = makeFakePiExecutable("t3-pi-text-generation-");
    const argsPath = path.join(tempDir, "args.json");
    const statePath = path.join(tempDir, "state.json");
    const textGeneration = yield* makePiTextGeneration(
      decodePiSettings({ binaryPath: fake.executable, enabled: true }),
      {
        instanceId: INSTANCE_ID,
        environment: {
          ...process.env,
          ...environment,
          PI_FAKE_CAPTURE_ARGS_FILE: argsPath,
          PI_FAKE_CAPTURE_STATE_FILE: statePath,
        },
      },
    );

    return yield* effectFn({ textGeneration, argsPath, statePath, fileSystem }).pipe(
      Effect.ensuring(
        Effect.sync(() => NodeFS.rmSync(fake.directory, { recursive: true, force: true })),
      ),
    );
  }).pipe(Effect.scoped, Effect.provide(testLayer));
}

const modelSelection = {
  instanceId: INSTANCE_ID,
  model: "gateway/org/model-v2",
  options: [{ id: "thinkingLevel", value: "high" as const }],
};

const commitInput = {
  cwd: process.cwd(),
  branch: "feature/pi-text",
  stagedSummary: "M README.md",
  stagedPatch: "diff --git a/README.md b/README.md",
  modelSelection,
};

describe("PiTextGeneration", () => {
  it.effect("generates and sanitizes commit messages through an isolated Pi session", () =>
    withFakePi(
      {
        PI_FAKE_TEXT_GENERATION_OUTPUT: JSON.stringify({
          subject: "  Add Pi metadata generation.\nignored",
          body: "\n- use an isolated session\n",
        }),
      },
      ({ textGeneration, argsPath, statePath, fileSystem }) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generateCommitMessage(commitInput);
          expect(generated).toEqual({
            subject: "Add Pi metadata generation",
            body: "- use an isolated session",
          });

          const args = JSON.parse(yield* fileSystem.readFileString(argsPath)) as string[];
          expect(args).toContain("--mode");
          expect(args).toContain("rpc");
          expect(args).toContain("--session-dir");
          expect(args).toContain("--session-id");
          expect(args).toContain("--no-tools");
          expect(args).toContain("--no-extensions");
          expect(args).toContain("--no-context-files");
          expect(args).not.toContain("--no-session");

          const state = JSON.parse(yield* fileSystem.readFileString(statePath)) as {
            currentModel: { provider: string; id: string };
            currentThinkingLevel: string;
            sessionFile: string;
          };
          expect(state.currentModel).toEqual({ provider: "gateway", id: "org/model-v2" });
          expect(state.currentThinkingLevel).toBe("high");
          expect(yield* fileSystem.exists(state.sessionFile)).toBe(false);
        }),
    ),
  );

  it.effect("supports PR content, branch names, and thread titles", () =>
    withFakePi(
      {
        PI_FAKE_TEXT_GENERATION_OUTPUT: JSON.stringify({
          title: "Improve Pi metadata",
          body: "\n## Summary\n- isolate metadata\n\n## Testing\n- Pi text generation\n",
          branch: "Feat/Pi-Metadata",
        }),
      },
      ({ textGeneration }) =>
        Effect.gen(function* () {
          const pr = yield* textGeneration.generatePrContent({
            cwd: process.cwd(),
            baseBranch: "main",
            headBranch: "feature/pi-text",
            commitSummary: "feat: add Pi text generation",
            diffSummary: "2 files changed",
            diffPatch: "diff --git a/a.ts b/a.ts",
            modelSelection,
          });
          expect(pr).toEqual({
            title: "Improve Pi metadata",
            body: "## Summary\n- isolate metadata\n\n## Testing\n- Pi text generation",
          });

          const branch = yield* textGeneration.generateBranchName({
            cwd: process.cwd(),
            message: "Add isolated Pi metadata generation",
            modelSelection,
          });
          expect(branch).toEqual({ branch: "feat/pi-metadata" });

          const title = yield* textGeneration.generateThreadTitle({
            cwd: process.cwd(),
            message: "Add isolated Pi metadata generation",
            modelSelection,
          });
          expect(title).toEqual({ title: "Improve Pi metadata" });
        }),
    ),
  );

  it.effect("passes image attachments through the Pi image-content conversion", () =>
    withFakePi(
      {
        PI_FAKE_TEXT_GENERATION_OUTPUT: JSON.stringify({ branch: "fix/image-context" }),
      },
      ({ textGeneration, statePath, fileSystem }) =>
        Effect.gen(function* () {
          const { attachmentsDir } = yield* ServerConfig.ServerConfig;
          const attachmentId = "pi-image-attachment";
          const attachmentPath = NodePath.join(attachmentsDir, `${attachmentId}.png`);
          yield* fileSystem.makeDirectory(attachmentsDir, { recursive: true });
          yield* fileSystem.writeFile(attachmentPath, Uint8Array.from([1, 2, 3, 4]));

          const result = yield* textGeneration.generateBranchName({
            cwd: process.cwd(),
            message: "Fix the screenshot layout",
            attachments: [
              {
                type: "image",
                id: attachmentId,
                name: "layout.png",
                mimeType: "image/png",
                sizeBytes: 4,
              },
            ],
            modelSelection,
          });
          expect(result).toEqual({ branch: "fix/image-context" });

          const state = JSON.parse(yield* fileSystem.readFileString(statePath)) as {
            images: Array<{ type: string; data: string; mimeType: string }>;
          };
          expect(state.images).toEqual([
            { type: "image", data: "AQIDBA==", mimeType: "image/png" },
          ]);
          yield* fileSystem.remove(attachmentPath);
        }),
    ),
  );

  it.effect("uses Pi's configured default model for the Pi-only provider fallback", () =>
    withFakePi(
      {
        PI_FAKE_TEXT_GENERATION_OUTPUT: JSON.stringify({ title: "Pi-only thread" }),
      },
      ({ textGeneration, statePath, fileSystem }) =>
        Effect.gen(function* () {
          for (const fallbackModel of ["auto", DEFAULT_TEXT_GENERATION_MODEL]) {
            const generated = yield* textGeneration.generateThreadTitle({
              cwd: process.cwd(),
              message: "Create a thread with Pi only",
              modelSelection: {
                instanceId: INSTANCE_ID,
                model: fallbackModel,
              },
            });
            expect(generated).toEqual({ title: "Pi-only thread" });
          }

          const state = JSON.parse(yield* fileSystem.readFileString(statePath)) as {
            currentModel: unknown;
          };
          expect(state.currentModel).toBeNull();
        }),
    ),
  );

  it.effect("turns empty, malformed, rejected, and interrupted output into typed errors", () =>
    Effect.gen(function* () {
      const empty = yield* withFakePi(
        { PI_FAKE_TEXT_GENERATION_EMPTY: "1" },
        ({ textGeneration }) =>
          textGeneration
            .generateThreadTitle({
              cwd: process.cwd(),
              message: "Name this thread",
              modelSelection,
            })
            .pipe(Effect.result),
      );
      expect(Result.isFailure(empty)).toBe(true);
      if (Result.isFailure(empty)) {
        expect(empty.failure).toBeInstanceOf(TextGenerationError);
        expect(empty.failure.detail).toContain("empty");
      }

      const malformed = yield* withFakePi(
        { PI_FAKE_TEXT_GENERATION_OUTPUT: "not JSON" },
        ({ textGeneration }) =>
          textGeneration
            .generateThreadTitle({
              cwd: process.cwd(),
              message: "Name this thread",
              modelSelection,
            })
            .pipe(Effect.result),
      );
      expect(Result.isFailure(malformed)).toBe(true);
      if (Result.isFailure(malformed)) {
        expect(malformed.failure).toBeInstanceOf(TextGenerationError);
        expect(malformed.failure.detail).toContain("invalid structured output");
      }

      const rejected = yield* withFakePi(
        {
          PI_FAKE_REJECT_FIRST_PROMPT: "1",
          PI_FAKE_TEXT_GENERATION_OUTPUT: JSON.stringify({ title: "ignored" }),
        },
        ({ textGeneration }) =>
          textGeneration
            .generateThreadTitle({
              cwd: process.cwd(),
              message: "Name this thread",
              modelSelection,
            })
            .pipe(Effect.result),
      );
      expect(Result.isFailure(rejected)).toBe(true);
      if (Result.isFailure(rejected)) {
        expect(rejected.failure).toBeInstanceOf(TextGenerationError);
        expect(rejected.failure.detail).toContain("rejected");
      }

      const interrupted = yield* withFakePi(
        {
          PI_FAKE_TEXT_GENERATION_STOP_REASON: "aborted",
          PI_FAKE_TEXT_GENERATION_OUTPUT: JSON.stringify({ title: "ignored" }),
        },
        ({ textGeneration }) =>
          textGeneration
            .generateThreadTitle({
              cwd: process.cwd(),
              message: "Name this thread",
              modelSelection,
            })
            .pipe(Effect.result),
      );
      expect(Result.isFailure(interrupted)).toBe(true);
      if (Result.isFailure(interrupted)) {
        expect(interrupted.failure).toBeInstanceOf(TextGenerationError);
        expect(interrupted.failure.detail).toContain("interrupted");
      }
    }),
  );
});
