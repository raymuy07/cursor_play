import logging
from pathlib import Path

import yaml
from openai import AsyncOpenAI

from app.common.utils import load_config

logger = logging.getLogger(__name__)

# Default prompts path
PROMPTS_PATH = Path(__file__).parent.parent.parent / "prompts" / "job_matching_v1.yaml"


class TextConverterAI:
    """Async text converter using OpenAI API with support for immediate and batch processing."""

    def __init__(self):
        self.config = load_config()
        self.client = AsyncOpenAI(api_key=self.config.get("openai_api_key"))
        self.prompts = self._load_prompts()

    async def run_through_llm(
        self,
        text: str,
        prompt_name: str,
    ) -> str:
        prompt_config = self.prompts[prompt_name]

        system_prompt = prompt_config["system_prompt"]
        user_message = prompt_config["user_template"].format(text=text)

        response = self.client.chat.completions.create(
            model=prompt_config.get("model", "gpt-4"),
            temperature=prompt_config.get("temperature", 0.2),
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_message}],
        )

        return response.choices[0].message.content

    def _load_prompts(self) -> dict:
        """Load prompts from YAML file."""
        with open(PROMPTS_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f)
