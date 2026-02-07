import json
from pathlib import Path

import pytest

from app.common.txt_converter_ai import TextConverterAI
from app.common.txt_embedder import TextEmbedder
from app.core.db_utils import JobsDB
from app.services.cv_manager import CVManager


def _job_text(job: dict) -> str:
    title = job.get("title") or ""
    desc = job.get("description") or ""
    if isinstance(desc, dict):
        desc = "\n".join(f"{k}: {v}" for k, v in desc.items() if v)
    return f"{title}\n{desc}".strip()


def _cosine(a: list[float], b: list[float]) -> float:
    import math

    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1e-9
    nb = math.sqrt(sum(x * x for x in b)) or 1e-9
    return dot / (na * nb)


# --- Temp section: run once to embed jobs, score CVs, freeze results. Then write pytest with assert. ---
FROZEN_DB = Path(__file__).parent.parent / "data" / "frozen_jobs.db"
CV_FILES_DIR = Path(__file__).parent.parent / "cv_files"
FROZEN_SCORES_FIXTURE = Path(__file__).parent / "fixtures" / "frozen_scores.json"


async def _run_frozen_scoring():
    """Embed jobs from frozen DB, score with a few CVs from cv_files, save to fixtures/frozen_scores.json."""
    if not FROZEN_DB.exists():
        pytest.skip(f"Frozen DB not found: {FROZEN_DB}. Run debug/dry_run.py once to create it.")
    if not CV_FILES_DIR.exists():
        pytest.skip(f"cv_files dir not found: {CV_FILES_DIR}")

    embedder = TextEmbedder()
    frozen = JobsDB(db_path=str(FROZEN_DB))
    await frozen.connect()
    jobs = await frozen.get_jobs_without_embeddings(limit=500)
    await frozen.close()

    # Embed jobs
    job_embeddings = {}
    for j in jobs:
        text = _job_text(j)
        if not text.strip():
            continue
        out = await embedder.embed_immediate(text)
        job_embeddings[j["url_hash"]] = out["embedding"]

    # CVs: use first few pdf/docx
    cv_paths = []
    for p in sorted(CV_FILES_DIR.iterdir()):
        if p.suffix.lower() in (".pdf", ".docx"):
            cv_paths.append(p)
            if len(cv_paths) >= 3:
                break

    if not cv_paths:
        pytest.skip("No PDF/DOCX in cv_files")

    converter = TextConverterAI()
    cv_manager = CVManager(embedder, converter)
    scores_by_cv = {}

    for cv_path in cv_paths:
        raw = cv_manager._extract_text(str(cv_path))
        raw = cv_manager._clean_text(raw)
        transformed = converter.run_through_llm(raw, "cv_to_job_description")
        out = await embedder.embed_immediate(transformed)
        cv_emb = out["embedding"]
        scores = {}
        for url_hash, job_emb in job_embeddings.items():
            scores[url_hash] = round(_cosine(cv_emb, job_emb), 4)
        scores_by_cv[cv_path.name] = scores

    FROZEN_SCORES_FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    with open(FROZEN_SCORES_FIXTURE, "w", encoding="utf-8") as f:
        json.dump(scores_by_cv, f, indent=2)
    return scores_by_cv


# @pytest.mark.prompt
# def test_frozen_scoring_save():
#     """Run once to freeze scores. Inspect fixture then write test_frozen_scoring_matches with assert."""
#     scores = asyncio.run(_run_frozen_scoring())
#     assert scores
#     print(f"Saved scores for {list(scores.keys())} to {FROZEN_SCORES_FIXTURE}")


# @pytest.mark.prompt
# def test_frozen_scoring_matches():
#     """Assert recomputed scores match frozen fixture. Expand with real tolerance assert once satisfied."""
#     if not FROZEN_SCORES_FIXTURE.exists():
#         pytest.skip("Run test_frozen_scoring_save once to create fixture")
#     with open(FROZEN_SCORES_FIXTURE, encoding="utf-8") as f:
#         frozen = json.load(f)
#     assert frozen
#     for _, job_scores in frozen.items():
#         assert isinstance(job_scores, dict)
#         assert all(isinstance(v, (int, float)) for v in job_scores.values())
# # --- end temp section ---


"""tmp to define the desired output of the prompt"""


"""actual pytest tests"""
# @pytest.fixture
# def text_converter_ai():


"""end of actual pytest tests"""
