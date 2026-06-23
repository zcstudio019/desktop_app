from __future__ import annotations

from typing import Any

__all__ = ["BankReceiptBundleAgent", "BankReceiptBundleAgentAdapter"]


def __getattr__(name: str) -> Any:
    if name == "BankReceiptBundleAgent":
        from .agent import BankReceiptBundleAgent

        return BankReceiptBundleAgent
    if name == "BankReceiptBundleAgentAdapter":
        from .adapter import BankReceiptBundleAgentAdapter

        return BankReceiptBundleAgentAdapter
    raise AttributeError(name)
