from datetime import datetime

from pydantic import BaseModel, Field


class ETArticle(BaseModel):
    url: str
    url_hash: str = ""
    title: str = ""
    summary: str = ""
    full_text: str = ""       # full article body
    author: str = ""
    category: str = ""        # e.g. "markets", "industry", "economy"
    sub_category: str = ""    # e.g. "stocks-news", "auto", "banking-finance"
    image_url: str = ""
    published_at: datetime | None = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    tags: list[str] = Field(default_factory=list)
    text_fetched: bool = False  # whether full_text has been scraped
