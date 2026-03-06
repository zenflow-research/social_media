from datetime import datetime

from pydantic import BaseModel, Field


class PIBRelease(BaseModel):
    prid: int
    title: str = ""
    ministry: str = ""
    published_at: datetime | None = None
    full_text: str = ""
    url: str = ""
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    analyzed: bool = False


class PIBAnalysis(BaseModel):
    prid: int
    summary: str = ""
    themes: list[dict] = Field(default_factory=list)
    affected_companies: list[dict] = Field(default_factory=list)
    sentiment: str = ""  # positive / negative / neutral
    impact_magnitude: str = ""  # high / medium / low
    key_policy_changes: list[str] = Field(default_factory=list)
    analyzed_at: datetime = Field(default_factory=datetime.utcnow)
    ministry: str = ""
    title: str = ""
    published_at: datetime | None = None
