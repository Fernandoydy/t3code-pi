import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeStringDecoder from "node:string_decoder";
import * as NodeURL from "node:url";

const decoder = new NodeStringDecoder.StringDecoder("utf8");

if (process.argv.includes("--version")) {
  process.stdout.write(`pi ${process.env.PI_FAKE_VERSION ?? "0.84.1"}\n`);
  process.exit(0);
}

const noSession = process.argv.includes("--no-session");
const sessionArgIndex = process.argv.indexOf("--session");
const requestedSessionFile = sessionArgIndex === -1 ? undefined : process.argv[sessionArgIndex + 1];
const fakeDirectory =
  process.env.PI_FAKE_SESSION_ROOT ??
  NodePath.join(
    NodeOS.tmpdir(),
    "t3-pi-rpc-fake",
    NodePath.basename(NodeURL.fileURLToPath(import.meta.url)),
  );
let sessionFile;
let sessionId;
let entries = [];
let nextEntrySequence = 0;

function failStartup(message) {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

function readSessionFile(filePath) {
  if (!NodeFS.existsSync(filePath)) {
    failStartup(`Native Pi session does not exist: ${filePath}`);
  }
  const contents = NodeFS.readFileSync(filePath, "utf8");
  if (!contents.trim()) {
    failStartup(`Native Pi session is empty: ${filePath}`);
  }
  try {
    const records = contents
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    const header = records[0];
    if (header?.type !== "session" || typeof header.id !== "string") {
      failStartup(`Native Pi session is corrupt: ${filePath}`);
    }
    sessionId = header.id;
    entries = records.slice(1);
    nextEntrySequence = entries.length;
  } catch {
    failStartup(`Native Pi session is corrupt: ${filePath}`);
  }
}

function appendSessionMessage(message) {
  if (!sessionFile) return;
  nextEntrySequence += 1;
  const entry = {
    type: "message",
    id: `fake-entry-${String(nextEntrySequence)}`,
    parentId: entries.at(-1)?.id ?? null,
    timestamp: new Date().toISOString(),
    message,
  };
  entries.push(entry);
  NodeFS.appendFileSync(sessionFile, `${JSON.stringify(entry)}\n`);
}

if (noSession) {
  sessionId = `fake-ephemeral-${String(process.pid)}`;
} else if (requestedSessionFile !== undefined) {
  sessionFile = NodePath.resolve(requestedSessionFile);
  readSessionFile(sessionFile);
} else {
  sessionId = process.env.PI_FAKE_SESSION_ID ?? `fake-session-${String(process.pid)}`;
  sessionFile = NodePath.resolve(
    process.env.PI_FAKE_SESSION_FILE ??
      NodePath.join(fakeDirectory, "native-sessions", `${sessionId}.jsonl`),
  );
  NodeFS.mkdirSync(NodePath.dirname(sessionFile), { recursive: true });
  NodeFS.writeFileSync(
    sessionFile,
    `${JSON.stringify({
      type: "session",
      version: 3,
      id: sessionId,
      timestamp: new Date().toISOString(),
      cwd: process.cwd(),
    })}\n`,
  );
}

let input = "";
let currentModel = null;
let currentThinkingLevel = "medium";
let promptCount = 0;
const deferredResponses = [];
const outputQueue = [];
let outputActive = false;

function flushOutputQueue() {
  const next = outputQueue.shift();
  if (next === undefined) {
    outputActive = false;
    return;
  }

  const finish = () => flushOutputQueue();
  if (!next.chunked) {
    process.stdout.write(next.bytes, finish);
    return;
  }

  let index = 0;
  const writeNextByte = () => {
    if (index >= next.bytes.length) {
      finish();
      return;
    }
    process.stdout.write(next.bytes.subarray(index, index + 1));
    index += 1;
    setImmediate(writeNextByte);
  };
  writeNextByte();
}

function writeOutput(value, chunked = false) {
  outputQueue.push({ bytes: Buffer.from(value), chunked });
  if (outputActive) return;
  outputActive = true;
  flushOutputQueue();
}

function writeJson(value, options = {}) {
  const line = `${JSON.stringify(value)}${options.crlf ? "\r\n" : "\n"}`;
  writeOutput(line, options.chunked === true);
}

function respond(command, data, options) {
  writeJson(
    {
      ...(command.id === undefined ? {} : { id: command.id }),
      type: "response",
      command: command.type,
      success: true,
      ...(data === undefined ? {} : { data }),
    },
    options,
  );
}

function reject(command, error) {
  writeJson({
    ...(command.id === undefined ? {} : { id: command.id }),
    type: "response",
    command: String(command.type),
    success: false,
    error,
  });
}

function handleCommand(command) {
  switch (command.type) {
    case "test_deferred_response":
      deferredResponses.push(command);
      if (deferredResponses.length === 1) {
        writeJson({ type: "deferred_received", id: command.id });
      }
      if (deferredResponses.length === 2) {
        for (const pending of deferredResponses.splice(0).toReversed()) {
          respond(pending, { value: pending.value });
        }
      }
      break;
    case "get_state":
      respond(command, {
        model: currentModel,
        thinkingLevel: currentThinkingLevel,
        isStreaming: false,
        isCompacting: false,
        steeringMode: "one-at-a-time",
        followUpMode: "one-at-a-time",
        ...(sessionFile === undefined ? {} : { sessionFile }),
        sessionId:
          process.env.PI_FAKE_STATE_ID_MISMATCH === "1" ? `${sessionId}-mismatch` : sessionId,
        autoCompactionEnabled: true,
        messageCount: 0,
        pendingMessageCount: 0,
      });
      break;
    case "set_model":
      currentModel = {
        provider: String(command.provider ?? "fake"),
        id: String(command.modelId ?? "fake-model"),
      };
      respond(command, currentModel);
      break;
    case "get_available_models":
      respond(command, {
        models:
          process.env.PI_FAKE_MODELS === "empty"
            ? []
            : process.env.PI_FAKE_MODELS === "capabilities"
              ? [
                  {
                    provider: "gateway",
                    id: "org/model/v2",
                    name: "Shared Model",
                    reasoning: true,
                  },
                  {
                    provider: "another",
                    id: "org/model/v2",
                    name: "Shared Model",
                    reasoning: true,
                  },
                  {
                    provider: "plain",
                    id: "text-only",
                    name: "Text Only",
                    reasoning: false,
                  },
                ]
              : [
                  {
                    provider: "fake",
                    id: "fake-model",
                    name: "Fake Model",
                    api: "fake",
                    baseUrl: "https://example.invalid",
                    reasoning: true,
                    input: ["text", "image"],
                    contextWindow: 128000,
                    maxTokens: 8192,
                    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
                  },
                ],
      });
      break;
    case "get_available_thinking_levels": {
      const modelSlug = currentModel ? `${currentModel.provider}/${currentModel.id}` : "";
      const levels =
        modelSlug === "gateway/org/model/v2"
          ? ["off", "high", "max"]
          : modelSlug === "another/org/model/v2"
            ? ["minimal", "medium", "xhigh"]
            : ["off", "minimal", "low", "medium", "high"];
      respond(command, { levels });
      break;
    }
    case "set_thinking_level":
      currentThinkingLevel = String(command.level ?? "off");
      respond(command);
      break;
    case "prompt": {
      promptCount += 1;
      if (process.env.PI_FAKE_REJECT_FIRST_PROMPT === "1" && promptCount === 1) {
        reject(command, "rejected first fake prompt");
        break;
      }
      const assistantText =
        process.env.PI_FAKE_SCENARIO === "basic-turn"
          ? `fake:${command.message ?? ""}:${process.cwd()}:${process.env.PI_FAKE_MARKER ?? ""}:${process.argv.slice(2).join(",")}`
          : process.env.PI_FAKE_SCENARIO === "model-selection"
            ? `selection:${currentModel?.provider ?? "none"}/${currentModel?.id ?? "none"}:${currentThinkingLevel}:${command.message ?? ""}`
            : `fake:${command.message ?? ""}`;
      respond(command);
      writeJson({ type: "agent_start" });
      if (process.env.PI_FAKE_WAIT_FOR_ABORT === "1") break;
      if (process.env.PI_FAKE_SCENARIO === "basic-turn") {
        writeJson({ type: "turn_start" });
        writeJson({ type: "message_start", message: { role: "assistant", content: [] } });
        writeJson({
          type: "message_update",
          assistantMessageEvent: {
            type: "thinking_delta",
            contentIndex: 0,
            delta: "considering",
          },
        });
        writeJson({
          type: "message_update",
          assistantMessageEvent: {
            type: "text_delta",
            contentIndex: 1,
            delta: assistantText,
          },
        });
        writeJson({
          type: "message_end",
          message: {
            role: "assistant",
            content: [
              { type: "thinking", thinking: "considering" },
              { type: "text", text: assistantText },
            ],
          },
        });
        writeJson({ type: "agent_end", messages: [], willRetry: false });
        writeJson({
          type: "tool_execution_start",
          toolCallId: "fake-tool-1",
          toolName: "bash",
          args: { command: "echo fake" },
        });
        writeJson({
          type: "tool_execution_update",
          toolCallId: "fake-tool-1",
          toolName: "bash",
          args: { command: "echo fake" },
          partialResult: { content: [{ type: "text", text: "fake" }] },
        });
        writeJson({
          type: "tool_execution_end",
          toolCallId: "fake-tool-1",
          toolName: "bash",
          result: { content: [{ type: "text", text: "fake" }] },
          isError: false,
        });
        writeJson({ type: "message_start", message: { role: "assistant", content: [] } });
        writeJson({
          type: "message_update",
          assistantMessageEvent: {
            type: "text_delta",
            contentIndex: 0,
            delta: "done",
          },
        });
        writeJson({
          type: "message_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "done" }],
            ...(process.env.PI_FAKE_TURN_RESULT === "error"
              ? { stopReason: "error", errorMessage: "fake model failure" }
              : {}),
          },
        });
      } else {
        writeJson({
          type: "message_update",
          assistantMessageEvent: {
            type: "text_delta",
            contentIndex: 0,
            delta: assistantText,
          },
        });
      }
      appendSessionMessage({
        role: "user",
        content: String(command.message ?? ""),
        timestamp: Date.now(),
      });
      appendSessionMessage({
        role: "assistant",
        content: [{ type: "text", text: assistantText }],
        provider: "fake",
        model: currentModel?.id ?? "fake-model",
        stopReason: process.env.PI_FAKE_TURN_RESULT === "error" ? "error" : "stop",
        timestamp: Date.now(),
      });
      if (process.env.PI_FAKE_MULTI_MESSAGE_HISTORY === "1") {
        appendSessionMessage({
          role: "toolResult",
          toolCallId: "fake-history-tool",
          toolName: "bash",
          content: [{ type: "text", text: "tool output" }],
          isError: false,
          timestamp: Date.now(),
        });
        appendSessionMessage({
          role: "assistant",
          content: [{ type: "text", text: "after tool" }],
          provider: "fake",
          model: currentModel?.id ?? "fake-model",
          stopReason: "stop",
          timestamp: Date.now(),
        });
      }
      writeJson({ type: "agent_settled" });
      break;
    }
    case "get_entries":
      respond(command, { entries, leafId: entries.at(-1)?.id ?? null });
      break;
    case "steer":
      respond(command);
      writeJson({ type: "queue_update", steering: [command.message ?? ""] });
      break;
    case "abort":
      respond(command);
      writeJson({ type: "agent_settled" });
      break;
    case "test_stderr":
      process.stderr.write(String(command.message ?? "fake diagnostic"));
      respond(command);
      break;
    case "test_unicode_separator":
      respond(command, { value: "before middle after" }, { chunked: true, crlf: true });
      writeJson({ type: "chunk_followup" });
      break;
    case "test_reject":
      reject(command, String(command.message ?? "fake rejection"));
      break;
    case "test_command_mismatch":
      writeJson({
        ...(command.id === undefined ? {} : { id: command.id }),
        type: "response",
        command: "different_command",
        success: true,
      });
      break;
    case "test_malformed_json":
      writeOutput('{"type":"broken"\n');
      break;
    case "test_oversized_record":
      writeOutput(`{"type":"oversized","value":"${"x".repeat(8 * 1024 * 1024)}"`);
      break;
    case "test_event_overflow":
      for (let index = 0; index <= 4096; index += 1) {
        writeJson({ type: "overflow_event", index });
      }
      respond(command);
      break;
    case "test_exit":
      process.exitCode = Number(command.code ?? 17);
      process.exit();
      break;
    case "test_never_respond":
      break;
    default:
      reject(command, `Unsupported fake command: ${String(command.type)}`);
  }
}

function processInput(chunk) {
  input += chunk;
  while (true) {
    const newlineIndex = input.indexOf("\n");
    if (newlineIndex === -1) return;
    let line = input.slice(0, newlineIndex);
    input = input.slice(newlineIndex + 1);
    if (line.endsWith("\r")) line = line.slice(0, -1);
    handleCommand(JSON.parse(line));
  }
}

process.stdin.on("data", (chunk) => processInput(decoder.write(chunk)));
process.stdin.on("end", () => {
  input += decoder.end();
  if (input.length > 0) handleCommand(JSON.parse(input));
});
