#!/usr/bin/env python3
"""GUI playground for comparing two files using embeddings and cosine similarity."""

import os
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import tkinter as tk
from tkinter import filedialog, messagebox

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.embed_cv import TextEmbedder, CVReader
from scripts.utils import load_config, setup_logging


def cosine_similarity(vec_a: np.ndarray, vec_b: np.ndarray) -> float:
    denom = float(np.linalg.norm(vec_a) * np.linalg.norm(vec_b))
    if denom == 0:
        raise ValueError("Cannot compute cosine similarity with zero-magnitude vectors")
    return float(np.dot(vec_a, vec_b) / denom)


def read_file_text(path: Path, logger) -> str:
    suffix = path.suffix.lower()
    if suffix in {".pdf", ".docx"}:
        logger.info("Extracting text from %s", path)
        reader = CVReader(str(path), logger=logger)
        return reader.extract_text()
    logger.info("Reading text file %s", path)
    return path.read_text(encoding="utf-8")


class EmbeddingPlaygroundApp:
    def __init__(self) -> None:
        self.logger = setup_logging()
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
            self.logger.warning("Unable to load config.yaml: %s", exc)
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
        if self.embedder is None:
            model_name = self._resolve_model()
            self.logger.info("Loading embedding model %s", model_name)
            self.embedder = TextEmbedder(model_name=model_name, logger=self.logger)
        return self.embedder

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

            embedding1 = embedder.embed_text(text1)
            embedding2 = embedder.embed_text(text2)

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
    main()

