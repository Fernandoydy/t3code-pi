import * as NodeStringDecoder from "node:string_decoder";

const decoder = new NodeStringDecoder.StringDecoder("utf8");
let input = "";
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
        model: null,
        thinkingLevel: "medium",
        isStreaming: false,
        isCompacting: false,
        steeringMode: "one-at-a-time",
        followUpMode: "one-at-a-time",
        sessionFile: "/tmp/fake-pi-session.jsonl",
        sessionId: "fake-session-1",
        autoCompactionEnabled: true,
        messageCount: 0,
        pendingMessageCount: 0,
      });
      break;
    case "get_available_models":
      respond(command, {
        models: [
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
    case "prompt":
      respond(command);
      writeJson({ type: "agent_start" });
      writeJson({
        type: "message_update",
        assistantMessageEvent: {
          type: "text_delta",
          contentIndex: 0,
          delta: `fake:${command.message ?? ""}`,
        },
      });
      writeJson({ type: "agent_settled" });
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
