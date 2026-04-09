# Alpha-Engine Chatbot: "The Tech Assistant" Proposal

## Overview
This document proposes a back-end AI module for the Alpha Engine. The chatbot acts strictly as a **stateless app technical support and navigation guide**. 

To maximize speed, prevent hallucinations, and completely eliminate architectural complexity, we are adopting a **Stateless Single-Call Architecture**. All orchestration loops, database dependencies, chat memory tracking, and multiple prompt arrays have been deeply simplified.

---

## Architecture: Fast In-Memory Context Injection

We compile context into exactly **ONE system message**. We do not construct complex message arrays or send multiple system roles. The final payload to OpenAI contains exactly two items: `[system, user]`.

### In-Memory Caching
To avoid expensive disk I/O on every call, static markdown files are loaded into memory once at application startup. Only the dynamic top-ten file is fetched on-demand (or periodically reloaded).

```python
# Conceptual implementation
PIPELINE = load("pipeline.md")
STRATEGIES = load("strategies.md")
UI = load("ui.md")

def get_context():
    top = load("top_ten.md")
    return PIPELINE + STRATEGIES + UI + top
```

### Local Text-to-Speech (Piper)
To provide a premium assistive experience, the Chatbot incorporates **Piper TTS**, a high-quality, fully local neural text-to-speech engine. The textual response from the LLM will be converted to audio offline and rendered seamlessly in the browser via a native Streamlit audio component (`st.audio`).

### Context Files (Final)
The exhaustive entirety of the agent's context relies on four files:
1. `pipeline.md`: Direct technical explanation of the data ingest, scoring, and prediction flow.
2. `strategies.md`: Core logic mapping of active strategies, their regimes, and distinct goals.
3. `ui.md`: Documentation of the Streamlit interface, explaining navigation and chart interpretation.
4. `top_ten.md` (Dynamic): A periodically dumped, highly compact summary of the current top-ten signals and strategies.

---

## Streamlit UI Design (Voice First)

Although built on Streamlit, the assistant is designed to feel like a **"voice-first"** companion rather than just a standard text box:

- **Voice Toggle Switch:** A persistent toggle switch in the UI allows users to easily turn the TTS audio on or off. By default, it expects a voice-first interaction.
- **Dynamic Audio Visualization:** To give the AI a "living" presence on screen, a simple, moving visualization (e.g., a pulsing CSS animation or Streamlit Lottie graphic) activates and loops simultaneously while Piper TTS's audio is playing.
- **Optional Text Chat UI:** The classic `st.chat_message` visual layout remains available. Users who prefer reading or need to copy/paste dense metric tables can interact entirely silently through the optional text interface without friction.

---

## Agent Persona Prompt (System Message)

The system prompt strictly combines the persona, the anti-hallucination constraints, and the raw file contents wrapped in demarcations:

```text
You are the advanced technical support and analytical guide for the Alpha Engine platform. Your primary purpose is to help the user navigate the app's interfaces, understand the data architecture, and grasp current strategy behaviors.

HARD CONSTRAINTS:
Only answer using provided context.
If the answer is not present, say: "Not available in Alpha Engine context."
Do not infer missing functionality.

--- pipeline.md ---
[... PIPELINE Content ...]

--- strategies.md ---
[... STRATEGIES Content ...]

--- ui.md ---
[... UI Content ...]

--- top_ten.md ---
[... TOP_TEN Content ...]
```

---

## Implementation Plan

1. **Context File Creation:**
   - Draft `pipeline.md`, `strategies.md`, and `ui.md` as static tech support guides. Keep them incredibly dense, direct, and clear.
   - **Dynamic Compilation (`scripts/compile_top_ten.py`)**: Create a single compiler step script that queries the `Predictions` table, sorts by signal strength (confidence), filters the top 10, and actively flushes them to `data/exports/top_ten.md`. This script should be triggered asynchronously after:
     1. Backfill completion.
     2. Live polling cycle finishes.
     3. Rankings are recomputed.
     4. Manual CLI execution.

2. **Streamlit Component Implementation (`app/ui/chatbot.py`):**
   - Cache static files in memory on startup.
   - Build the monolithic string combining the Persona, Hard Constraints, and File Contexts.
   - Send the pure `[system, user]` message block to `openai.chat.completions.create`.
   - Stream the text response directly to `st.write_stream` via `st.chat_message`. Do not save memory of the interaction.
   - Simultaneously spawn a background Piper TTS process to render the response text to a `.wav` file, and seamlessly play it using `st.audio(..., format='audio/wav', autoplay=True)` within the chat window.

## Summary 
By keeping the context exclusively in-memory, constraining the payload to a single rigid system message, and applying strict anti-hallucination directives, this chatbot variant is purely deterministic. It executes at maximum speed while preventing speculative or hallucinated answers.
