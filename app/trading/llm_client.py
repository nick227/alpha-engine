import os
import json
import logging
import asyncio
from typing import Dict, Any, Optional

try:
    from openai import AsyncOpenAI  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    AsyncOpenAI = None  # type: ignore

logger = logging.getLogger(__name__)

class LLMClient:
    """
    Async client for LLM validation services.
    Currently supports OpenAI.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.model = config.get("model", "gpt-4o-mini")
        self.timeout = config.get("timeout", 10.0)
        
        if not self.api_key:
            logger.warning("OPENAI_API_KEY not found in environment. LLM validation will fail-safe.")
            self.client = None
        elif AsyncOpenAI is None:
            logger.warning("openai package not installed. LLM validation will fail-safe.")
            self.client = None
        else:
            self.client = AsyncOpenAI(api_key=self.api_key)
            
    async def validate_signal(self, prompt: str) -> Optional[Dict[str, Any]]:
        """
        Send a signal validation prompt to the LLM.
        Returns the parsed JSON response or None on failure.
        """
        if not self.client:
            return None
            
        try:
            response = await asyncio.wait_for(
                self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a professional trading analyst."},
                        {"role": "user", "content": prompt}
                    ],
                    response_format={"type": "json_object"}
                ),
                timeout=self.timeout
            )
            
            content = response.choices[0].message.content
            return json.loads(content)
            
        except asyncio.TimeoutError:
            logger.error("LLM validation timed out")
            return None
        except Exception as e:
            logger.error(f"LLM validation failed: {e}")
            return None

    @staticmethod
    def format_prompt(template_path: str, data: Dict[str, Any]) -> str:
        """Helper to fill template with signal data."""
        try:
            with open(template_path, 'r') as f:
                template = f.read()
            
            # Simple replacement
            for key, value in data.items():
                placeholder = "{{" + key + "}}"
                template = template.replace(placeholder, str(value))
                
            return template
        except Exception as e:
            logger.error(f"Failed to format LLM prompt: {e}")
            return ""
