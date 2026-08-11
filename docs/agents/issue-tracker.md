# Issue tracker: GitHub

Issues and PRDs for this repository live as GitHub issues. Use the `gh` CLI for all operations and infer the repository from `git remote -v`.

## Conventions

- Create an issue with `gh issue create --title "..." --body "..."`.
- Read an issue with `gh issue view <number> --comments` and fetch its labels.
- List issues with `gh issue list`, selecting bodies, labels, and comments as needed.
- Comment with `gh issue comment <number> --body "..."`.
- Apply or remove labels with `gh issue edit`.
- Close issues with `gh issue close` and a closing comment when useful.

Use a heredoc for multiline bodies.

## Pull requests as a triage surface

External pull requests are not a request or triage surface. Triage only GitHub Issues.

## Skill operations

- When a skill says to publish to the issue tracker, create a GitHub issue.
- When a skill says to fetch a ticket, read the corresponding GitHub issue and its comments.
- GitHub Issues are the source of truth for specs and implementation tickets.

## Wayfinding

When `/wayfinder` is used, represent the map as one GitHub issue and investigations as child issues. Prefer native GitHub sub-issues and issue dependencies; if unavailable, use task lists and explicit `Blocked by: #<number>` relationships. A ticket is ready only when all blockers are closed and it is unassigned.
