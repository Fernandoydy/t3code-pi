# Pi Agent

Pi Agent (Pi) is an external coding-agent harness that T3 Code drives as a provider. This guide
covers installing and authenticating Pi, what T3 Code controls, and the capabilities Pi does not
expose to T3 Code. For first-time setup, see [Install T3 Code](./install.md). For multi-account
setups with other providers, see [Codex](./providers-codex.md) and [Claude](./providers-claude.md).

## Pi stays in charge of Pi

T3 Code does not bundle Pi. The desktop app ships without the Pi SDK and without a bundled `pi`
executable; T3 Code drives the `pi` you install, over Pi's RPC protocol. Pi remains the source of
truth for its own installation, authentication, credentials, configuration, extensions, skills,
prompt templates, and native session files. T3 Code renders what Pi reports and never duplicates
Pi's state.

## Install Pi

Pi must be installed on the machine that runs the T3 Code server, and version **0.84.1 or
newer** is required. Install it the same way you would use it from a terminal:

```bash
npm install -g --ignore-scripts @earendil-works/pi-coding-agent
```

or with the official installer:

```bash
curl -fsSL https://pi.dev/install.sh | sh
```

T3 Code warns in provider settings when the installed Pi is older than 0.84.1.

## Point T3 Code at the executable

The default **Binary path** is `pi`, resolved from the `PATH` of the shell that started the T3
Code server. On Windows, an npm-style install provides `pi.cmd` and T3 Code resolves it the same
way `cmd.exe` would.

Set an explicit **Binary path** when Pi is installed somewhere outside that `PATH`, for example
through a version manager or a portable install. The path is used verbatim, so on Windows point
at the `.cmd` (or `.exe`) file itself.

## Authenticate Pi

Run `pi` in a terminal on the server machine and use `/login` to pick a provider — a
subscription (Anthropic Claude, OpenAI, GitHub Copilot) or an API key. T3 Code does not store or
manage Pi credentials.

Provider status in **Settings** reflects Pi's own state: when Pi reports models, the provider
shows as ready and authenticated; when Pi has no models, T3 Code says to configure
authentication and models in Pi itself.

## Environment and project trust

Pi runs with the environment of the T3 Code server process, plus any per-provider environment
overrides you add in **Settings**. Pi keeps its own files:

- Global configuration, credentials, and trust decisions live in `~/.pi/agent/`.
- Project-local resources (`settings.json`, prompts, skills, extensions) live in `.pi/` folders
  inside each project.

When Pi starts interactively, it asks before trusting a project that contains project-local
settings or skills. T3 Code always starts Pi non-interactively, where Pi never shows a trust
prompt: it applies `defaultProjectTrust` from `~/.pi/agent/settings.json`, and the default
`ask` ignores project-local `.pi` resources until the project is trusted.

To use a project's `.pi` resources from T3 Code, run `pi` in that project once and use `/trust`,
or set `defaultProjectTrust` to `"always"` in `~/.pi/agent/settings.json`. `AGENTS.md` /
`CLAUDE.md` context files are always loaded, trusted or not.

## What T3 Code shows from Pi

- **Model picker**: Pi's native provider/model identities and supported thinking levels. You can
  switch models within an existing thread, and Pi applies the switch.
- **Slash-command picker**: Pi's native commands — extension commands, prompt templates, and
  skills — with Pi's own descriptions. Skills keep their `skill:` prefix and insert as
  `/skill:name`; Pi performs its own expansion, so commands behave exactly as they do inside Pi.
  Commands that only exist in Pi's interactive terminal are not listed.
- **Extension dialogs**: Pi's select, confirm, input, and editor dialogs surface through T3
  Code's structured input prompts, and Pi warning and error notifications appear in the thread.
- **Updates**: when Pi reports a newer version, T3 Code shows the advisory in provider settings.
  For a recognized package-managed installation, **Update now** runs Pi's native `pi update
--self` (or the matching package-manager update). Only Pi itself is touched; Pi controls its
  own extensions, packages, credentials, and settings. Custom installs update manually from the
  installation.
- **Metadata**: Pi generates thread titles and source-control text (commit messages, branch
  names, pull-request content) in isolated, tool-disabled sessions that never touch the chat
  conversation.

## What Pi does not expose in T3 Code

- **Permission modes**: Pi does not expose permission requests to this integration. Pi's design
  has no permission popups, so T3 Code's approval-based modes cannot pause or gate Pi's tool
  calls — a Pi thread behaves like a full-access thread regardless of the mode shown in the
  composer. T3 Code hides the runtime-mode and plan-mode controls for Pi for this reason. Run Pi
  the way you would in a terminal: in a container, a sandbox, or a throwaway worktree.
- **Plan mode**: Pi has no built-in plan mode (it is an extension feature in Pi), so T3 Code's
  plan interaction mode has no effect on Pi threads.
- **MCP servers**: Pi's core has no MCP support, and T3 Code's MCP servers are not bridged into
  Pi sessions. Pi uses skills, prompt templates, and extensions instead.
- **Generic file attachments**: only image attachments are sent to Pi, for multimodal models.
  Other file attachments are not sent into Pi turns.
- **Background-agent UI**: Pi is a single-agent harness and does not spawn subagents, so T3
  Code's subagent fleet surfaces (the Agents panel) never apply to Pi threads; Pi threads run as
  ordinary threads.
- **Provider-side rollback**: Pi sessions cannot be rolled back to an earlier native state. T3
  Code checkpoints still restore workspace files, but the Pi conversation itself is not rewound.
