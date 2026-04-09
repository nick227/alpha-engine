from __future__ import annotations

from app.ui.chatbot import render_chat_assistant


def render_chat_panel() -> None:
    render_chat_assistant(show_title=False, input_key="top_bar_chat_question")
