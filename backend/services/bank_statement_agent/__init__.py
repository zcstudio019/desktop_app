from __future__ import annotations

from typing import Any

__all__ = ["BankStatementAgent", "BankStatementAgentAdapter"]


def __getattr__(name: str) -> Any:
    if name == "BankStatementAgent":
        from .agent import BankStatementAgent

        return BankStatementAgent
    if name == "BankStatementAgentAdapter":
        from .adapter import BankStatementAgentAdapter

        return BankStatementAgentAdapter
    raise AttributeError(name)
