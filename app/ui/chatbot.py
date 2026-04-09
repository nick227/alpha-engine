from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import streamlit as st


@dataclass(frozen=True)
class ContextDoc:
    name: str
    path: Path


DOCS: list[ContextDoc] = [
    ContextDoc("pipeline", Path("app/ai/context/pipeline.md")),
    ContextDoc("strategies", Path("app/ai/context/strategies.md")),
    ContextDoc("ui", Path("app/ai/context/ui.md")),
    ContextDoc("admin", Path("ADMIN_GUIDE.md")),
]


def _read_docs() -> tuple[dict[str, str], list[str]]:
    docs: dict[str, str] = {}
    missing: list[str] = []
    for d in DOCS:
        try:
            docs[d.name] = d.path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            missing.append(str(d.path))
    return docs, missing


def _tokenize(text: str) -> list[str]:
    tokens = re.findall(r"[a-zA-Z0-9_]{2,}", text.lower())
    return [t for t in tokens if t not in {"the", "and", "for", "with", "this", "that", "from", "into", "what", "how"}]


def _top_snippets(docs: dict[str, str], query: str, *, max_snippets: int = 6) -> list[tuple[str, str]]:
    q = query.strip()
    if not q:
        return []

    q_tokens = set(_tokenize(q))
    if not q_tokens:
        return []

    scored: list[tuple[int, str, str]] = []
    for name, content in docs.items():
        for line in content.splitlines():
            l = line.strip()
            if not l:
                continue
            score = sum(1 for t in q_tokens if t in l.lower())
            if score:
                scored.append((score, name, l))

    scored.sort(key=lambda x: (x[0], len(x[2])), reverse=True)
    out: list[tuple[str, str]] = []
    seen = set()
    for _, name, line in scored:
        key = (name, line)
        if key in seen:
            continue
        seen.add(key)
        out.append((name, line))
        if len(out) >= max_snippets:
            break
    return out


def render_chat_assistant(*, show_title: bool = True, input_key: str = "chat_assistant_question") -> None:
    if show_title:
        st.markdown("# Chat Assistant")
        st.caption("Stateless helper: answers only from local markdown context files.")
    else:
        st.caption("Stateless helper: answers only from local markdown context files.")

    docs, missing = _read_docs()
    if missing:
        st.warning("Missing context files (assistant still works with remaining docs):")
        for m in missing:
            st.code(m)

    with st.expander("What this assistant can do", expanded=False):
        st.markdown(
            """
- App navigation help (where to find Dashboard / IH / Audit / Backtest views)
- Explain fields like confidence / alpha / efficiency (from project docs)
- Explain what a page is showing (from UI docs)

It is **stateless**: no conversation history is stored or used.
"""
        )

    prompt = st.text_input(
        "Question",
        placeholder="e.g. What does confidence mean? Where is the Signal Audit?",
        key=input_key,
    )
    if not prompt:
        return

    snippets = _top_snippets(docs, prompt)
    if not snippets:
        st.info("Not available in Alpha Engine context.")
        return

    st.markdown("## Answer (from context)")
    st.write(
        "I can’t generate new facts here; these are the most relevant context lines I found. "
        "If you want, rephrase with more specific keywords (page name, table name, metric name)."
    )

    for name, line in snippets:
        st.markdown(f"- **{name}**: {line}")


def chatbot_main() -> None:
    render_chat_assistant(show_title=True, input_key="chat_assistant_full_page_question")


if __name__ == "__main__":
    st.set_page_config(page_title="Chat Assistant", layout="wide", page_icon="💬")
    chatbot_main()
