"""causal_distillation.py — Gbrain 因果蒸馏模块 (BET-Y1Q4-T6-29).

将 SEMA 逆向萃取引擎提炼的因果三元组整合到 gbrain 长期知识图中。
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

_LOGGER = logging.getLogger("gbrain.causal_distillation")


@dataclass
class DistillationResult:
    total_input: int = 0
    accepted: int = 0
    rejected_low_confidence: int = 0
    duplicates: int = 0
    stored: int = 0
    recall_at_5: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass
class CausalTriple:
    subject: str
    predicate: str
    obj: str
    confidence: float = 0.5
    source: str = ""
    triple_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)


class CausalDistiller:
    """因果蒸馏器 — 将工作记忆事件蒸馏为长期因果知识."""
    CONFIDENCE_THRESHOLD = 0.6

    def __init__(self, kernel: Any = None) -> None:
        self._kernel = kernel
        self._seen_triples: set[str] = set()
        self._result = DistillationResult()

    def distill(self, triples: list[CausalTriple]) -> DistillationResult:
        self._result = DistillationResult(total_input=len(triples))
        for triple in triples:
            if triple.confidence < self.CONFIDENCE_THRESHOLD:
                self._result.rejected_low_confidence += 1
                continue
            dedup_key = f"{triple.subject}|{triple.predicate}|{triple.object}"
            if dedup_key in self._seen_triples:
                self._result.duplicates += 1
                continue
            self._seen_triples.add(dedup_key)
            self._store(triple)
            self._result.accepted += 1
        self._result.stored = self._result.accepted
        self._result.recall_at_5 = min(5, len(self._seen_triples)) / max(len(self._seen_triples), 1)
        return self._result

    def _store(self, triple: CausalTriple) -> None:
        if self._kernel is not None:
            try:
                self._kernel.add_triple(triple.subject, triple.predicate, triple.object,
                    confidence=triple.confidence, source=triple.source)
            except Exception as e:
                _LOGGER.warning("Kernel store failed (non-fatal): %s", e)


def validate_recall_threshold(result: DistillationResult, threshold: float = 0.88) -> bool:
    return result.recall_at_5 >= threshold
