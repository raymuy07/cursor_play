#!/usr/bin/env python3
"""GUI playground for comparing two files using embeddings and cosine similarity."""

import os
import sys
from pathlib import Path
from typing import Optional
import random
import numpy as np
import tkinter as tk
from tkinter import filedialog, messagebox
from db_utils import JobsDB
from typing import List, Dict, Optional
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.utils import TextEmbedder
from scripts.embed_cv import CVProcessor
from common.utils import load_config, setup_logging


logger = logging.getLogger(__name__)


def cosine_similarity(vec_a: np.ndarray, vec_b: np.ndarray) -> float:
    denom = float(np.linalg.norm(vec_a) * np.linalg.norm(vec_b))
    if denom == 0:
        raise ValueError("Cannot compute cosine similarity with zero-magnitude vectors")
    return float(np.dot(vec_a, vec_b) / denom)



def sample_batch_of_jobs(jobs_db: JobsDB, text_embedder: TextEmbedder, cv_embedding: dict) -> List[Dict]:
    jobs_score_list = []

    jobs = jobs_db.get_jobs_without_embeddings(limit=50)
    sample_size = min(50, len(jobs))  # Handle case where < 50 jobs exist
    random_jobs = random.sample(jobs, sample_size)


    for i, job in enumerate(random_jobs):
        # Jobs are dicts, not objects - use dict access
        job_title = job.get('title', 'Unknown')
        job_description = job.get('description', '')
        job_id = job.get('id')
        company = job.get('company_name', 'Unknown')


        job_embedding = text_embedder.embed(job_description)

        similarity_score = cosine_similarity(cv_embedding['embedding'], job_embedding['embedding'])

        jobs_score_list.append({
                'id': job_id,
                'title': job_title,
                'company': company,
                'description': job_description[:500],  # Truncate for output
                'similarity_score': similarity_score
            })

    jobs_score_list.sort(key=lambda x: x['similarity_score'], reverse=True)

    # Save results to output file
    output_file = Path(r"C:\Users\Guy\Desktop\taker_texts_expiremtn") / "job_rankings.txt"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"CV: {cv_embedding['source_file']}\n")
        f.write(f"Jobs analyzed: {len(jobs_score_list)}\n")
        f.write("=" * 60 + "\n\n")

        for rank, job in enumerate(jobs_score_list, 1):
            f.write(f"#{rank} | Score: {job['similarity_score']:.4f}\n")
            f.write(f"    {job['title']} @ {job['company']}\n")
            f.write(f"    {job['description'][:200]}...\n\n")

    logger.info("Results saved to %s", output_file)

    return jobs_score_list



class EmbeddingPlaygroundApp:
    def __init__(self) -> None:
        self.config = self._load_config()
        self.embedder: Optional[TextEmbedder] = None

        self.root = tk.Tk()
        self.root.title("Embedding Playground")
        self.root.geometry("520x220")
        self.root.resizable(False, False)

        self.file1_var = tk.StringVar()
        self.file2_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Select two files to compare.")

        self._build_ui()

    def _load_config(self) -> dict:
        try:
            return load_config()
        except Exception as exc:
            logger.warning("Unable to load config.yaml: %s", exc)
            return {}

    def _resolve_model(self) -> str:
        return (
            self.config.get("job_embedding", {}).get("model_name")
            or self.config.get("cv_embedding", {}).get("model_name")
            or "all-MiniLM-L6-v2"
        )

    def _build_ui(self) -> None:
        padding = {"padx": 12, "pady": 6}

        file1_frame = tk.Frame(self.root)
        file1_frame.pack(fill=tk.X, **padding)
        tk.Label(file1_frame, text="File #1:").pack(side=tk.LEFT)
        tk.Entry(file1_frame, textvariable=self.file1_var, width=50).pack(side=tk.LEFT, padx=6)
        tk.Button(file1_frame, text="Browse", command=lambda: self._browse_file(self.file1_var)).pack(side=tk.LEFT)

        file2_frame = tk.Frame(self.root)
        file2_frame.pack(fill=tk.X, **padding)
        tk.Label(file2_frame, text="File #2:").pack(side=tk.LEFT)
        tk.Entry(file2_frame, textvariable=self.file2_var, width=50).pack(side=tk.LEFT, padx=6)
        tk.Button(file2_frame, text="Browse", command=lambda: self._browse_file(self.file2_var)).pack(side=tk.LEFT)

        action_frame = tk.Frame(self.root)
        action_frame.pack(fill=tk.X, **padding)
        tk.Button(action_frame, text="Embed & Compare", command=self._compare).pack()

        status_frame = tk.Frame(self.root)
        status_frame.pack(fill=tk.X, **padding)
        tk.Label(status_frame, textvariable=self.status_var, fg="blue").pack()

    def _browse_file(self, target_var: tk.StringVar) -> None:
        path = filedialog.askopenfilename(
            filetypes=[
                ("Supported", "*.pdf *.docx *.txt"),
                ("PDF", "*.pdf"),
                ("Word", "*.docx"),
                ("Text", "*.txt"),
                ("All files", "*.*"),
            ]
        )
        if path:
            target_var.set(path)

    def _ensure_files(self) -> tuple[Path, Path]:
        file1 = Path(self.file1_var.get()).expanduser()
        file2 = Path(self.file2_var.get()).expanduser()
        if not file1.exists() or not file2.exists():
            raise FileNotFoundError("Both selected files must exist.")
        return file1, file2

    def _load_embedder(self) -> TextEmbedder:
        client = OpenAI(api_key=self.config.get('openai_api_key'))
        return client
        # if self.embedder is None:
        #     model_name = self._resolve_model()
        #     self.logger.info("Loading embedding model %s", model_name)
        #     self.embedder = TextEmbedder(model_name=model_name, logger=self.logger)
        # return self.embedder

    def _compare(self) -> None:
        try:
            file1, file2 = self._ensure_files()
            self.status_var.set("Loading files...")
            self.root.update_idletasks()

            text1 = read_file_text(file1, self.logger)
            text2 = read_file_text(file2, self.logger)

            if not text1.strip() or not text2.strip():
                raise ValueError("Both files must contain text to embed.")

            embedder = self._load_embedder()
            self.status_var.set("Generating embeddings...")
            self.root.update_idletasks()

            embedding1 = embedder.embeddings.create(input=text1, model="text-embedding-3-small").data[0].embedding
            embedding2 = embedder.embeddings.create(input=text2, model="text-embedding-3-small").data[0].embedding

            # embedding1 = embedder.embed_text(text1)
            # embedding2 = embedder.embed_text(text2)

            score = cosine_similarity(embedding1, embedding2)
            self.status_var.set(f"Cosine similarity: {score:.6f}")
        except Exception as exc:
            self.logger.warning("Comparison failed: %s", exc)
            messagebox.showerror("Error", str(exc))
            self.status_var.set("Comparison failed. See logs for details.")

    def run(self) -> None:
        self.root.mainloop()


def main() -> None:
    app = EmbeddingPlaygroundApp()
    app.run()


if __name__ == "__main__":


    sample_cv_path = r"C:\Users\Guy\Desktop\taker_texts_expiremtn\CV-Ofek_Ben_Shlush.pdf"
    cv_reader = CVProcessor()
    text_embedder = TextEmbedder()
    cv_embedding = cv_reader.process(sample_cv_path)
    sample_batch_of_jobs(JobsDB(), text_embedder, cv_embedding)  # JobsDB() not JobsDB
