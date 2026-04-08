"""Lightweight local embedding helpers for RAG MVP."""

from __future__ import annotations

import hashlib
import math
import re
from collections import Counter


class EmbeddingService:
    """Generate deterministic local embeddings without external dependencies."""

    def __init__(self, dimension: int = 128):
        self.dimension = dimension

    def tokenize(self, text: str) -> list[str]:
        normalized = (text or "").lower()
        return [token for token in re.findall(r"[\u4e00-\u9fff]+|[a-z0-9]+", normalized) if token]

    def embed_text(self, text: str) -> list[float]:
        tokens = self.tokenize(text)
        if not tokens:
            return [0.0] * self.dimension

        counts = Counter(tokens)
        vector = [0.0] * self.dimension
        for token, count in counts.items():
            bucket = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16) % self.dimension
            vector[bucket] += float(count)

        norm = math.sqrt(sum(value * value for value in vector))
        if norm == 0:
            return vector
        return [value / norm for value in vector]

    def cosine_similarity(self, left: list[float], right: list[float]) -> float:
        if not left or not right or len(left) != len(right):
            return 0.0
        return float(sum(a * b for a, b in zip(left, right)))
