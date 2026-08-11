# Install T3 Code

T3 Code is a web and desktop GUI for running coding agents on your machine.

## Requirements

Node.js `^22.16 || ^23.11 || >=24.10` on the machine that runs the T3 Code server.

At least one provider CLI, installed and authenticated. See [Providers](#providers) below.

## Run Without Installing

```bash
npx t3@latest
```

This starts the T3 Code server on your machine and opens the local web app. Use
`npx t3@latest --help` for the full CLI reference.

## Desktop App

Download the latest release from
[GitHub Releases](https://github.com/pingdotgg/t3code/releases), or install from a package
registry.

Windows:

```bash
winget install T3Tools.T3Code
```

macOS:

```bash
brew install --cask t3-code
```

Arch Linux:

```bash
yay -S t3code-bin
```

## Providers

T3 Code drives provider CLIs; it does not ship them. Install the CLI for each provider you want
to use, then authenticate it.

| Provider   | CLI                                                   | Default binary | Log in with           |
| ---------- | ----------------------------------------------------- | -------------- | --------------------- |
| Codex      | [Codex CLI](https://developers.openai.com/codex/cli)  | `codex`        | `codex login`         |
| Claude     | [Claude Code](https://claude.com/product/claude-code) | `claude`       | `claude auth login`   |
| Cursor     | [Cursor CLI](https://cursor.com/cli)                  | `cursor-agent` | `agent login`         |
| Grok Build | [Grok Build CLI](https://x.ai/cli)                    | `grok`         | `grok login`          |
| OpenCode   | [OpenCode](https://opencode.ai)                       | `opencode`     | `opencode auth login` |
| Pi Agent   | [Pi Agent](https://pi.dev)                            | `pi`           | `pi`, then `/login`   |

Cursor is the one to watch: install Cursor CLI, which provides the `cursor-agent` binary that
T3 Code looks for, but authenticate with `agent login`, not `cursor-agent login`.

Run the login command on the machine running the T3 Code server, not on the device you browse
from.

Pi Agent requires version 0.84.1 or newer. T3 Code uses Pi's existing models, credentials,
settings, project-trust decisions, and native session storage; configure those in Pi itself. The
model picker shows Pi's native provider/model identities and supported thinking levels, and can
switch models within an existing thread. Pi-backed threads resume their exact native conversation
after a server restart or idle cleanup. Sending another message while a Pi turn is active steers
that turn, and stopping it interrupts the native Pi turn without ending the session. Image
attachments are sent to multimodal Pi models in both new messages and steering messages.
Interactive Pi extensions surface their select, confirm, input, and editor dialogs through T3
Code's structured input prompts, and their warning and error notifications appear in the thread.
The slash-command picker shows Pi's native commands — extension commands, prompt templates,
and skills — with Pi's own descriptions, where skills keep their `skill:` prefix and insert as
`/skill:name`. Selecting a command inserts the raw slash prompt and Pi performs its own
expansion, so commands behave exactly as they do inside Pi; commands that only exist in Pi's
interactive terminal are not listed.
Pi can also generate thread titles and source-control text (commit messages, branch names, and
pull-request content) in isolated, tool-disabled sessions that never touch the active chat
conversation. Pi does not expose T3 Code's permission or plan-mode controls.

When Pi reports a newer version, T3 Code shows the advisory in provider settings. For a recognized
package-managed installation, **Update now** runs Pi's native `pi update --self` command (or the
matching global package-manager update when the install maps to one). Only Pi itself is touched;
extensions, packages, credentials, settings, and model catalogs remain under Pi's control. If the
executable uses a custom installation path that T3 Code cannot classify, use Pi's own updater
manually from that installation.

### Binary Discovery

Each provider CLI must be on the server's `PATH`, or have an explicit binary path set in
**Settings** → the provider instance → **Binary path**. Use the explicit path when a version
manager or a non-standard install location keeps the CLI off the `PATH` of the shell that
started T3 Code.

### When Auth Is Needed

Provider auth is required before you start a session with that provider, not before you start
T3 Code. You can install T3 Code, open it, and add providers afterwards. A provider that is not
authenticated shows its status in **Settings** and fails at session start with the login command
to run.

For multi-account setups, see [Codex](./providers-codex.md) and [Claude](./providers-claude.md).

## Next Steps

- [Permission modes](./permission-modes.md): how much T3 Code asks before acting
- [Remote access](./remote-access.md): connect from a phone, tablet, or another desktop
- [Keeping T3 Code in sync](./updating.md): client and server version skew
- [Running in the background](./background-service.md): Linux background service
