import json
import logging
import os
import pickle
import tempfile

from openai import AsyncOpenAI

from common.utils import load_config

logger = logging.getLogger(__name__)


class TextEmbedder:
    """Async text embedder using OpenAI API with support for immediate and batch processing."""

    def __init__(self):
        self.config = load_config()
        self.model_name = self.config.get("embeddings", {}).get("model_name")
        self.client = AsyncOpenAI(api_key=self.config.get("openai_api_key"))

    async def embed_immediate(self, text: str) -> dict:
        """
        Generate embedding for the given text immediately.
        Use this for single/few embeddings when you need results right away.
        """
        if not text or not text.strip():
            raise ValueError("Cannot generate embeddings for empty text")

        try:
            response = await self.client.embeddings.create(input=text, model=self.model_name)
            embedding = response.data[0].embedding
            logger.debug(f"Embedding generated successfully. Dimension: {len(embedding)}")

            return {
                "embedding": embedding,
                "model_name": self.model_name,
            }

        except Exception as e:
            raise RuntimeError(f"Error generating embedding: {e}") from e

    def _create_batch_jsonl(self, texts: list[str], output_path: str) -> str:
        """Create a JSONL file for batch embedding requests."""
        tasks = []

        for index, text in enumerate(texts):
            task = {
                "custom_id": f"embed-{index}",
                "method": "POST",
                "url": "/v1/embeddings",
                "body": {"model": self.model_name, "input": text},
            }
            tasks.append(task)

        with open(output_path, "w", encoding="utf-8") as file:
            for task in tasks:
                file.write(json.dumps(task) + "\n")

        logger.debug(f"Created batch JSONL file with {len(tasks)} tasks at {output_path}")
        return output_path

    async def create_embedding_batch(self, texts: list[str]) -> str:
        """
        Create a batch job for embedding multiple texts.
        Uses OpenAI Batch API - 50% cheaper, results within 24h.
        Returns:
            batch_id: The ID of the created batch job
        """
        if not texts:
            raise ValueError("Cannot create batch for empty text list")

        # Create temporary JSONL file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as tmp:
            jsonl_path = tmp.name

        try:
            self._create_batch_jsonl(texts, jsonl_path)
            # Upload the file to OpenAI
            with open(jsonl_path, "rb") as file:
                batch_file = await self.client.files.create(file=file, purpose="batch")

            logger.info(f"Uploaded batch file: {batch_file.id}")

            batch_job = await self.client.batches.create(
                input_file_id=batch_file.id, endpoint="/v1/embeddings", completion_window="24h"
            )

            logger.info(f"Created batch job: {batch_job.id} with {len(texts)} texts")
            return batch_job.id

        finally:
            # Clean up temp file
            if os.path.exists(jsonl_path):
                os.unlink(jsonl_path)

    async def get_batch_status(self, batch_id: str) -> dict:
        """
        Check the status of a batch job.
        """
        batch = await self.client.batches.retrieve(batch_id)
        return {
            "status": batch.status,
            "completed": batch.request_counts.completed if batch.request_counts else 0,
            "failed": batch.request_counts.failed if batch.request_counts else 0,
            "total": batch.request_counts.total if batch.request_counts else 0,
            "output_file_id": batch.output_file_id,
            "error_file_id": batch.error_file_id,
        }

    async def get_batch_results(self, batch_id: str) -> dict[str, list[float]]:
        """
        Retrieve results from a completed batch job.
        """
        batch = await self.client.batches.retrieve(batch_id)

        if batch.status != "completed":
            raise ValueError(f"Batch not complete, status: {batch.status}")

        if not batch.output_file_id:
            raise ValueError("Batch completed but no output file available")

        # Download results
        result_content = await self.client.files.content(batch.output_file_id)

        embeddings = {}
        for line in result_content.text.strip().split("\n"):
            if not line:
                continue
            result = json.loads(line)
            custom_id = result["custom_id"]
            # Extract index from custom_id (e.g., "embed-5" -> 5)
            embedding = result["response"]["body"]["data"][0]["embedding"]
            embeddings[custom_id] = embedding

        logger.info(f"Retrieved {len(embeddings)} embeddings from batch {batch_id}")
        return embeddings

    def save_embedding(self, embedding_data: dict, output_path: str):
        """
        Save embedding data to a pickle file.

        Args:
            embedding_data: dictionary containing embedding and metadata
            output_path: Path where to save the pickle file
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Save to pickle file
            with open(output_path, "wb") as f:
                pickle.dump(embedding_data, f)

            logger.info(f"Embedding saved successfully to: {output_path}")

            # Print summary
            file_size = os.path.getsize(output_path) / 1024  # KB
            logger.info(f"File size: {file_size:.2f} KB")

        except Exception as e:
            raise RuntimeError(f"Error saving embedding: {e}") from e
