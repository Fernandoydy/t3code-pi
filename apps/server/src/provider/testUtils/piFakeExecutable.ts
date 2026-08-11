// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const fakePiScript = NodePath.join(__dirname, "../testFixtures/piRpcFake.mjs");

export function makeFakePiExecutable(prefix: string) {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), prefix));
  if (NodePath.sep === "\\") {
    const executable = NodePath.join(directory, "pi.cmd");
    NodeFS.writeFileSync(
      executable,
      `@echo off\r\nset "PI_FAKE_SESSION_ROOT=${directory}"\r\n"${process.execPath}" "${fakePiScript}" %*\r\n`,
      "utf8",
    );
    return { directory, executable };
  }

  const executable = NodePath.join(directory, "pi");
  NodeFS.writeFileSync(
    executable,
    `#!/bin/sh\nPI_FAKE_SESSION_ROOT="${directory}" exec "${process.execPath}" "${fakePiScript}" "$@"\n`,
    { encoding: "utf8", mode: 0o755 },
  );
  return { directory, executable };
}
