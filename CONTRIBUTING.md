---
type: ssot
owner: governance-team
last_updated: 2026-09-03
---

# Contributing to bus-foundation

> Omni-Bus Data/Event/Control interfaces
> Omni-Bus Data/Event/Control 三平面接口

Thank you for considering a contribution! This guide covers the development workflow, commit conventions, and review process for **bus-foundation**.

## Development Environment

- **Stack**: Python (uv, pytest)
- **Python requirement**: see [`pyproject.toml`](pyproject.toml) (if applicable)

## Quick Start

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest "tests/" -q

# Run lint
uv run ruff check "src/"

# Format code
uv run ruff format "src/"
```

## Commit Convention

Follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat(scope):` — New feature
- `fix(scope):` — Bug fix
- `refactor(scope):` — Code refactoring
- `docs(scope):` — Documentation
- `test(scope):` — Tests
- `chore(scope):` — Maintenance

## Code Standards

- Keep changes focused and scoped to one concern per PR.
- Add or update tests for new behavior.
- Ensure the lint/test commands above pass before requesting review.

## Project-Specific Notes

- Keep the three planes (Data, Event, Control) cleanly separated in changes.
- Update `docs/CAPABILITY-MAP.md` when adding new bus capabilities.

## Pull Request Process

1. Create a feature branch from `main`.
2. Make changes following the conventions above.
3. Run the project's test and lint commands.
4. Submit a PR with a clear description of the change and its motivation.

## Getting Help

- See [`AGENTS.md`](AGENTS.md) for AI-agent developer rules.
- See [`CLAUDE.md`](CLAUDE.md) for session startup context.
- Workspace architecture: [`../../ARCHITECTURE.md`](../../ARCHITECTURE.md)
