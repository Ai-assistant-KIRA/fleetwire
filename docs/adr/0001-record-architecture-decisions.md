# ADR-0001: Record architecture decisions

- Status: accepted
- Date: 2026-11-09

## Context

fleetwire makes a strong promise (every reading stored exactly once, tenants
kept apart) and most of the code exists to keep it. The reasons behind the
design choices are easy to lose: why acks are end to end, why the KEK lives
outside the database, why ingest does not need ordered input. A reviewer or a
new contributor needs those reasons next to the code, not in someone's memory.

## Decision

We keep Architecture Decision Records in `docs/adr/`, numbered in order, using
`docs/adr/template.md`. Each record has context, decision, alternatives
considered, and consequences. A change that makes or reverses a main design
decision adds or updates its ADR in the same commit. ADRs are not edited to
rewrite history; a reversed decision gets a new ADR that supersedes the old
one.

## Alternatives considered

- **Design document only.** `design.md` explains how the system works, but it
  is rewritten as the system changes, so it loses the alternatives and the
  reasons a choice was made.
- **Wiki or issue tracker.** Decisions drift away from the code version they
  describe and are not reviewed with the change.

## Consequences

- Reviews of design changes include the ADR diff.
- The ADR list is the short reading list for a new contributor.
- Writing an ADR costs a little time per decision; we accept that.
