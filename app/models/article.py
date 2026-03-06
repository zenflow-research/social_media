from datetime import datetime

from pydantic import BaseModel, Field


class Article(BaseModel):
    url: str
    url_hash: str = ""
    title: str = ""
    summary: str = ""
    source_name: str = ""
    source_type: str = ""  # e.g. "web", "twitter", "rss", "linkedin"
    published_at: datetime | None = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    tags: list[str] = Field(default_factory=list)
    trending_topics: list[str] = Field(default_factory=list)
    raw_content: str = ""
    image_url: str = ""
    extra: dict = Field(default_factory=dict)
