"""Tests for bus_foundation.topics — SSOT topic catalog (Round 0)."""

from __future__ import annotations

import re

import pytest

from bus_foundation import topics


def test_constants_are_strings() -> None:
    """Every topic constant must be a non-empty string."""
    for name in topics.__all__:
        if name.startswith("PATTERN_"):
            continue
        value = getattr(topics, name)
        assert isinstance(value, str), f"{name} is not str: {type(value)}"
        assert value, f"{name} is empty"


def test_topics_match_hierarchical_convention() -> None:
    """Topics must be lowercase, colon-or-dot-separated, non-empty segments.

    Allow a few pre-existing agora wire-format topics that use a
    mix of colon and dot separators (e.g. registry:service.registered)
    to remain compatible with existing publishers/consumers. New
    topics must follow `domain:category[:...]` lowercase convention.
    """
    strict_pattern = re.compile(r"^[a-z][a-z0-9_]*(:[a-z][a-z0-9_]*)+$")
    # Pre-existing wire-format topics (lower bar: only require
    # non-empty lowercase + colon-or-dot separator + no whitespace).
    legacy_allowed = {
        "REGISTRY_SERVICE_REGISTERED",  # "registry:service.registered"
    }
    for name in topics.__all__:
        if name.startswith("PATTERN_"):
            continue
        value = getattr(topics, name)
        if name in legacy_allowed:
            # Looser check: only non-empty + no whitespace + no uppercase.
            assert value == value.lower(), f"{name}={value!r} has uppercase"
            assert " " not in value, f"{name}={value!r} has whitespace"
            continue
        assert strict_pattern.match(value), f"{name}={value!r} does not match convention"


def test_patterns_use_globs() -> None:
    """Pattern constants must end with .* / :* or be '*' (legacy support)."""
    for name in topics.__all__:
        if not name.startswith("PATTERN_"):
            continue
        value = getattr(topics, name)
        assert value.endswith(".*") or value.endswith(":*") or value == "*", f"{name}={value!r} invalid pattern"


def test_no_duplicate_values() -> None:
    """All topic values must be unique (no accidental shadowing)."""
    seen: dict[str, str] = {}
    for name in topics.__all__:
        value = getattr(topics, name)
        if value in seen:
            pytest.fail(f"Duplicate topic value {value!r} for {name} and {seen[value]}")
        seen[value] = name


def test_pipeline_and_governance_exist() -> None:
    """Smoke check: core pipeline + governance topics are present."""
    assert topics.PIPELINE_COMPLETED == "pipeline:completed"
    assert topics.DEBT_CREATED == "debt:created"
    assert topics.PATTERN_PIPELINE_ALL == "pipeline:*"
    assert topics.PATTERN_DEBT_ALL == "debt:*"


def test_aetherforge_topics_use_swarm_prefix() -> None:
    """aetherforge topics must use swarm:* or mesh:* prefix (not aetherforge:*)."""
    swarm_topics = [
        topics.SWARM_WORKER_HATCHED,
        topics.SWARM_HATCHER_INITIALIZED,
        topics.MESH_NODE_STATUS_CHANGED,
    ]
    for t in swarm_topics:
        assert t.startswith(("swarm:", "mesh:")), f"{t} does not use swarm/mesh prefix"


def test_metaos_topics_use_metaos_prefix() -> None:
    """metaos topics must use metaos:* prefix."""
    for name in [
        "METAOS_NODE_PENDING",
        "METAOS_NODE_RUNNING",
        "METAOS_NODE_COMPLETED",
        "METAOS_NODE_FAILED",
        "METAOS_NODE_AWAITING_APPROVAL",
    ]:
        value = getattr(topics, name)
        assert value.startswith("metaos:"), f"{name}={value!r} does not use metaos: prefix"
