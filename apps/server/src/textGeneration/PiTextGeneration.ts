import {
  DEFAULT_TEXT_GENERATION_MODEL,
  ProviderInstanceId,
  TextGenerationError,
  type ModelSelection,
  type PiSettings,
} from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { loadPiImageAttachments } from "../provider/pi/PiAttachments.ts";
import {
  decodePiThinkingLevel,
  parsePiModelSlug,
  PI_THINKING_LEVEL_OPTION_ID,
} from "../provider/pi/PiModel.ts";
import { makePiRpcProcess, type PiRpcWireMessage } from "../provider/pi/PiRpcProcess.ts";
import * as ServerConfig from "../config.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

const PI_TEXT_GENERATION_TIMEOUT_MS = 180_000;
const PI_TEXT_GENERATION_OPERATIONS = [
  "generateCommitMessage",
  "generatePrContent",
  "generateBranchName",
  "generateThreadTitle",
] as const;
type PiTextGenerationOperation = (typeof PI_TEXT_GENERATION_OPERATIONS)[number];

const decodeOutput = <S extends Schema.Top>(schema: S) =>
  Schema.decodeEffect(Schema.fromJsonString(schema));
const PiLastAssistantText = Schema.Struct({ text: Schema.NullOr(Schema.String) });
const decodePiLastAssistantText = Schema.decodeUnknownEffect(PiLastAssistantText);

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function assistantTextFromContent(content: unknown): string {
  if (!Array.isArray(content)) return "";
  return content
    .flatMap((block) => {
      if (!isRecord(block) || block.type !== "text" || typeof block.text !== "string") return [];
      return [block.text];
    })
    .join("");
}

function assistantMessageFromEvent(event: PiRpcWireMessage) {
  if (event.type !== "message_end" || !isRecord(event.message)) return undefined;
  if (event.message.role !== "assistant") return undefined;
  return event.message;
}

function textGenerationErrorDetail(operation: PiTextGenerationOperation, cause: unknown) {
  if (cause instanceof TextGenerationError) return cause;

  if (isRecord(cause) && cause._tag === "PiRpcCommandRejectedError") {
    return new TextGenerationError({
      operation,
      detail:
        typeof cause.detail === "string"
          ? `Pi Agent rejected the text-generation prompt: ${cause.detail}`
          : "Pi Agent rejected the text-generation prompt.",
      cause,
    });
  }

  if (isRecord(cause) && cause._tag === "PiRpcProcessExitError") {
    const stderr = typeof cause.stderr === "string" ? cause.stderr.trim() : "";
    return new TextGenerationError({
      operation,
      detail:
        stderr.length > 0
          ? `Pi Agent text-generation process failed: ${stderr}`
          : "Pi Agent text-generation process exited unexpectedly.",
      cause,
    });
  }

  if (cause instanceof Error && cause.message.length > 0) {
    return new TextGenerationError({
      operation,
      detail: `Pi Agent text generation failed: ${cause.message}`,
      cause,
    });
  }

  return new TextGenerationError({
    operation,
    detail: "Pi Agent text generation failed.",
    cause,
  });
}

export interface PiTextGenerationOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

export const makePiTextGeneration = Effect.fn("makePiTextGeneration")(function* (
  settings: PiSettings,
  options?: PiTextGenerationOptions,
) {
  const serverConfig = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const instanceId = options?.instanceId ?? ProviderInstanceId.make("piAgent");
  const environment = options?.environment ?? process.env;

  const applyModelSelection = Effect.fn("PiTextGeneration.applyModelSelection")(function* (
    rpc: Effect.Success<ReturnType<typeof makePiRpcProcess>>,
    operation: PiTextGenerationOperation,
    modelSelection: ModelSelection,
  ) {
    if (modelSelection.instanceId !== instanceId) {
      return yield* new TextGenerationError({
        operation,
        detail: `Pi Agent model selection is bound to '${modelSelection.instanceId}', expected '${instanceId}'.`,
      });
    }

    const rawModel = modelSelection.model.trim();
    const nativeModel = parsePiModelSlug(rawModel);
    // Server settings fall back to "auto" when Pi is the only enabled
    // provider, and older persisted settings may still carry the historical
    // global text-generation model. In both cases Pi's own configured default
    // is the only meaningful model selection.
    if (!nativeModel && rawModel !== "auto" && rawModel !== DEFAULT_TEXT_GENERATION_MODEL) {
      return yield* new TextGenerationError({
        operation,
        detail: "Pi Agent model selection must use the 'provider/model' format.",
      });
    }

    const rawThinkingLevel = getModelSelectionStringOptionValue(
      modelSelection,
      PI_THINKING_LEVEL_OPTION_ID,
    );
    const thinkingLevel =
      rawThinkingLevel === undefined
        ? undefined
        : Option.getOrUndefined(decodePiThinkingLevel(rawThinkingLevel));
    if (rawThinkingLevel !== undefined && thinkingLevel === undefined) {
      return yield* new TextGenerationError({
        operation,
        detail: `Unsupported Pi Agent thinking level '${rawThinkingLevel}'.`,
      });
    }

    if (nativeModel) {
      yield* rpc.request({
        type: "set_model",
        provider: nativeModel.provider,
        modelId: nativeModel.modelId,
      });
    }
    if (thinkingLevel !== undefined) {
      yield* rpc.request({ type: "set_thinking_level", level: thinkingLevel });
    }
  });

  const runPiJson = Effect.fn("PiTextGeneration.runPiJson")(function* <
    S extends Schema.Top,
  >(input: {
    readonly operation: PiTextGenerationOperation;
    readonly cwd: string;
    readonly prompt: string;
    readonly outputSchemaJson: S;
    readonly modelSelection: ModelSelection;
    readonly attachments?: TextGeneration.BranchNameGenerationInput["attachments"];
  }) {
    const run = Effect.gen(function* () {
      const sessionDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: `t3code-pi-text-${process.pid}-`,
      });
      const sessionId = `t3-text-${yield* crypto.randomUUIDv4}`;
      const images = yield* loadPiImageAttachments({
        attachments: input.attachments,
        attachmentsDir: serverConfig.attachmentsDir,
        fileSystem,
      });
      const rpcExit = yield* makePiRpcProcess({
        command: settings.binaryPath || "pi",
        args: [
          "--mode",
          "rpc",
          "--session-dir",
          sessionDir,
          "--session-id",
          sessionId,
          "--no-tools",
          "--no-extensions",
          "--no-skills",
          "--no-prompt-templates",
          "--no-themes",
          "--no-context-files",
        ],
        cwd: path.resolve(input.cwd),
        environment,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
        Effect.exit,
      );
      if (Exit.isFailure(rpcExit)) {
        return yield* Effect.failCause(rpcExit.cause);
      }
      const rpc = rpcExit.value;

      yield* applyModelSelection(rpc, input.operation, input.modelSelection);
      const eventsFiber = yield* rpc.events.pipe(
        Stream.takeUntil((event) => event.type === "agent_settled"),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* rpc.request({
        type: "prompt",
        message: input.prompt,
        ...(images.length > 0 ? { images } : {}),
      });

      const events = yield* Fiber.join(eventsFiber);
      let deltaText = "";
      let finalText: string | undefined;
      let terminalError: string | undefined;
      let interrupted = false;

      for (const event of events) {
        if (event.type === "message_update" && isRecord(event.assistantMessageEvent)) {
          if (
            event.assistantMessageEvent.type === "text_delta" &&
            typeof event.assistantMessageEvent.delta === "string"
          ) {
            deltaText += event.assistantMessageEvent.delta;
          }
        }

        const message = assistantMessageFromEvent(event);
        if (message) {
          finalText = assistantTextFromContent(message.content);
          if (message.stopReason === "error") {
            terminalError =
              typeof message.errorMessage === "string" && message.errorMessage.trim().length > 0
                ? message.errorMessage
                : "Pi Agent returned an error while generating text.";
          } else if (message.stopReason === "aborted") {
            interrupted = true;
          }
        }
      }

      if (terminalError !== undefined) {
        return yield* new TextGenerationError({
          operation: input.operation,
          detail: terminalError,
        });
      }
      if (interrupted) {
        return yield* new TextGenerationError({
          operation: input.operation,
          detail: "Pi Agent text generation was interrupted.",
        });
      }

      const lastAssistantTextResponse = yield* rpc.request({ type: "get_last_assistant_text" });
      const lastAssistantText = yield* decodePiLastAssistantText(
        lastAssistantTextResponse.data,
      ).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation: input.operation,
              detail: "Pi Agent returned an invalid last-assistant-text response.",
              cause,
            }),
        ),
      );
      const rawOutput = (lastAssistantText.text ?? finalText ?? deltaText).trim();
      if (rawOutput.length === 0) {
        return yield* new TextGenerationError({
          operation: input.operation,
          detail: "Pi Agent returned empty text-generation output.",
        });
      }

      return yield* decodeOutput(input.outputSchemaJson)(extractJsonObject(rawOutput)).pipe(
        Effect.catchTags({
          SchemaError: (cause) =>
            Effect.fail(
              new TextGenerationError({
                operation: input.operation,
                detail: "Pi Agent returned invalid structured output.",
                cause,
              }),
            ),
        }),
      );
    }).pipe(
      Effect.mapError((cause) => textGenerationErrorDetail(input.operation, cause)),
      Effect.scoped,
      Effect.timeoutOption(PI_TEXT_GENERATION_TIMEOUT_MS),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new TextGenerationError({
                operation: input.operation,
                detail: "Pi Agent text-generation request timed out.",
              }),
            ),
          onSome: (value) => Effect.succeed(value),
        }),
      ),
    );

    return yield* run;
  });

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("PiTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });
      const generated = yield* runPiJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("PiTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });
      const generated = yield* runPiJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("PiTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });
      const generated = yield* runPiJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
        attachments: input.attachments,
      });
      return { branch: sanitizeBranchFragment(generated.branch) };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("PiTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });
      const generated = yield* runPiJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
        attachments: input.attachments,
      });
      return { title: sanitizeThreadTitle(generated.title) };
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
