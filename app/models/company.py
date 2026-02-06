"""
Pydantic models for data validation across the pipeline.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator

logger = logging.getLogger(__name__)


class CompanyModel(BaseModel):
    """
    Validated company model for the pipeline.

    Ensures companies have required fields and valid data
    before being persisted to the database.
    """

    company_name: str = Field(..., min_length=1, description="Company name")
    domain: str = Field(..., min_length=1, description="Company domain")
    company_page_url: HttpUrl = Field(..., description="Company page URL")
    title: str | None = None
    discovered_at: datetime = Field(default_factory=datetime.now, description="Date and time the company was discovered")
    last_scraped: datetime | None = None
    is_active: bool = Field(default=True, description="Whether the company is active")
    source: str = Field(default="google_serper", description="Source of the company")

    @field_validator("company_name")
    @classmethod
    def company_name_must_not_be_empty(cls, v: str) -> str:
        """Ensure company name is not just whitespace."""
        stripped = v.strip()
        if not stripped:
            raise ValueError("company name cannot be empty or whitespace")
        return stripped

    @field_validator("domain")
    @classmethod
    def domain_must_not_be_empty(cls, v: str) -> str:
        """Ensure domain is not just whitespace."""
        stripped = v.strip()
        if not stripped:
            raise ValueError("domain cannot be empty or whitespace")
        return stripped

    @field_validator("company_page_url", mode="before")
    @classmethod
    def url_must_be_valid(cls, v: Any) -> Any:
        """Ensure URL is a valid string before HttpUrl parsing."""
        if v is None or (isinstance(v, str) and not v.strip()):
            raise ValueError("company_page_url cannot be empty")
        return v

    @model_validator(mode="after")
    def validate_company_has_identifiable_info(self) -> CompanyModel:
        """Ensure company has enough info to be useful."""
        return self

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for queue/database operations."""
        data = self.model_dump()
        data["company_page_url"] = str(self.company_page_url)
        return data

    class Config:
        extra = "ignore"
