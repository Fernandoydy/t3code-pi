# Domain docs

This repository uses a single domain context.

## Before exploring

Read these when they exist:

- `CONTEXT.md` at the repository root;
- relevant ADRs under `docs/adr/`;
- `docs/internals/glossary.md`, the existing T3 Code glossary.

If `CONTEXT.md` or `docs/adr/` does not exist, proceed silently. Domain-modeling skills create them lazily when terminology or architectural decisions need to be recorded.

## Vocabulary

Use the terms defined by `CONTEXT.md` and `docs/internals/glossary.md` in specs, tickets, tests, and implementation notes. Avoid introducing synonyms for established concepts.

When a necessary concept is missing, either reconsider whether new vocabulary is needed or record the gap for domain modeling.

## Architectural decisions

Read ADRs relevant to the area being changed. If proposed work conflicts with an existing ADR, identify the conflict explicitly instead of silently overriding the earlier decision.
