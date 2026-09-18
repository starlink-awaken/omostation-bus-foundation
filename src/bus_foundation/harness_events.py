from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

HarnessEventType = Literal[
    "harness:arch:upgrade",
    "harness:feature:add",
    "harness:bug:fix",
    "harness:experience:optimize",
    "harness:doc:governance",
    "harness:toolchain:update",
    "harness:business:process",
]


@dataclass(frozen=True)
class HarnessEvent:
    type: HarnessEventType
    source: str
    spec_ref: str
    metrics: dict
    north_star_link: str
    context: dict
