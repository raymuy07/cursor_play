"""
Pydantic models for data validation across the pipeline.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator

logger = logging.getLogger(__name__)


class JobModel(BaseModel):
    """
    Validated job model for the scraping pipeline.

    This model ensures jobs have required fields and valid data
    before being published to the queue or persisted to the database.
    """

    # Required fields
    title: str = Field(..., min_length=1, description="Job title")
    url: HttpUrl = Field(..., description="Job posting URL")

    # Optional fields with defaults
    company_name: str | None = None
    department: str | None = None
    location: str | None = None
    employment_type: str | None = None
    experience_level: str | None = None
    workplace_type: str | None = None
    uid: str | None = None
    email: str | None = None
    last_updated: str | None = None
    original_website_job_url: str | None = None
    description: dict[str, Any] | str | None = None

    @field_validator("title")
    @classmethod
    def title_must_not_be_empty(cls, v: str) -> str:
        """Ensure title is not just whitespace."""
        stripped = v.strip()
        if not stripped:
            raise ValueError("title cannot be empty or whitespace")
        return stripped

    @field_validator("url", mode="before")
    @classmethod
    def url_must_be_valid(cls, v: Any) -> Any:
        """Ensure URL is a valid string before HttpUrl parsing."""
        if v is None or (isinstance(v, str) and not v.strip()):
            raise ValueError("url cannot be empty")
        return v

    @field_validator("company_name", "department", "location", mode="before")
    @classmethod
    def strip_string_fields(cls, v: Any) -> Any:
        """Strip whitespace from string fields."""
        if isinstance(v, str):
            stripped = v.strip()
            return stripped if stripped else None
        return v

    @model_validator(mode="after")
    def validate_job_has_identifiable_info(self) -> JobModel:
        """Ensure job has enough info to be useful."""
        # A job should have at least a title and URL (already enforced by required fields)
        # Additional business logic can go here
        return self

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for queue/database operations."""
        data = self.model_dump()
        # Convert HttpUrl back to string for serialization
        data["url"] = str(self.url)
        return data

    class Config:
        # Allow extra fields to be passed through (for forward compatibility)
        extra = "ignore"


def validate_jobs(raw_jobs: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Validate a list of raw job dictionaries.

    Returns:
        Tuple of (valid_jobs, invalid_jobs) where:
        - valid_jobs: list of validated job dicts ready for the pipeline
        - invalid_jobs: list of dicts with {'job': original_dict, 'error': str}
    """
    valid_jobs = []
    invalid_jobs = []

    for raw_job in raw_jobs:
        try:
            validated = JobModel(**raw_job)
            valid_jobs.append(validated.to_dict())
        except Exception as e:
            invalid_jobs.append({"job": raw_job, "error": str(e)})
            logger.debug(f"Job validation failed: {e}")

    if invalid_jobs:
        logger.warning(f"Validation failed for {len(invalid_jobs)}/{len(raw_jobs)} jobs")

    return valid_jobs, invalid_jobs
