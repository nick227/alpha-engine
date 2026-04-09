import streamlit as st
import os
import time
import subprocess
from pathlib import Path
import openai

# Setup API Key (Assuming dotenv or set up elsewhere, falling back to os path for this snippet)
client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY", "your-api-key"))

# Directories
BASE_DIR = Path(__file__).parent.parent.parent
CONTEXT_DIR = BASE_DIR / "app" / "ai" / "context"
EXPORTS_DIR = BASE_DIR / "data" / "exports"
AUDIO_DIR = BASE_DIR / "outputs" / "audio"

AUDIO_DIR.mkdir(parents=True, exist_ok=True)

@st.cache_data
def load_static_context():
    """Load the monolithic static markdown files into memory only once."""
    pipeline = (CONTEXT_DIR / "pipeline.md").read_text(encoding="utf-8", errors="ignore")
    strategies = (CONTEXT_DIR / "strategies.md").read_text(encoding="utf-8", errors="ignore")
    ui = (CONTEXT_DIR / "ui.md").read_text(encoding="utf-8", errors="ignore")
    return pipeline, strategies, ui

def get_dynamic_context():
    """Load the frequently changing top-ten file on demand."""
    try:
        return (EXPORTS_DIR / "top_ten.md").read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return "Top ten metrics unavailable."

def build_system_message():
    """Constructs the pure, single static monolithic system prompt."""
    p_pipe, p_strat, p_ui = load_static_context()
    p_top = get_dynamic_context()
    
    return f"""You are the advanced technical support and analytical guide for the Alpha Engine platform. Your primary purpose is to help the user navigate the app's interfaces, understand the data architecture, and grasp current strategy behaviors.

HARD CONSTRAINTS:
Only answer using provided context.
If the answer is not present, say: "Not available in Alpha Engine context."
Do not infer missing functionality.

--- pipeline.md ---
{p_pipe}

--- strategies.md ---
{p_strat}

--- ui.md ---
{p_ui}

--- top_ten.md ---
{p_top}
"""

def generate_tts(text: str, output_path: str):
    """
    Generates audio using Piper TTS. 
    Assumes piper is installed and accessible in the system PATH.
    """
    try:
        # Example command using piper CLI
        # Note: You need a local model e.g., en_US-lessac-medium.onnx
        model_path = os.environ.get("PIPER_MODEL_PATH", "en_US-lessac-medium.onnx")
        
        # We shell out to the piper binary
        process = subprocess.Popen(
            ['piper', '--model', model_path, '--output_file', output_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        process.communicate(input=text.encode('utf-8'))
        return True
    except Exception as e:
        st.error(f"TTS Engine Error: {e}")
        return False

def render_chatbot():
    st.title("The Tech Assistant")
    
    # UI Toggles
    col1, col2 = st.columns([3,1])
    with col1:
        st.caption("Alpha Engine Quant & Navigation Helper")
    with col2:
        voice_mode = st.toggle("Voice Mode", value=True)
    
    st.divider()

    # The interaction loop (stateless)
    user_input = st.chat_input("Ask about the pipeline, strategies, or UI navigation...")
    
    if user_input:
        # 1. Display user msg
        with st.chat_message("user"):
            st.write(user_input)

        # 2. Build purely stateless API call Payload
        system_content = build_system_message()
        messages = [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_input}
        ]
        
        # 3. Stream Inference & Update UI
        with st.chat_message("assistant"):
            if voice_mode:
                st.write("*(Voice mode engaged - Rendering response...)*")
            
            # OpenAI streamed response
            stream = client.chat.completions.create(
                model="gpt-4o",  # or gpt-3.5-turbo / local model
                messages=messages,
                stream=True
            )
            response_text = st.write_stream(stream)
            
            # 4. Handle Text-to-Speech (Piper)
            if voice_mode and response_text:
                audio_file = str(AUDIO_DIR / "latest_response.wav")
                
                # Show dynamic visualization placeholder
                with st.spinner("Synthesizing Voice..."):
                    success = generate_tts(response_text, audio_file)
                
                if success and Path(audio_file).exists():
                    st.audio(audio_file, autoplay=True, format="audio/wav")

if __name__ == "__main__":
    render_chatbot()
