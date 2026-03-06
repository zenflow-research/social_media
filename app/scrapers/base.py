import abc
import hashlib
import logging
from datetime import datetime

from app.database import get_db
from app.models.article import Article
from app.services.keyword_filter import tag_article

logger = logging.getLogger(__name__)


class BaseScraper(abc.ABC):
    source_name: str = "unknown"
    source_type: str = "web"

    @abc.abstractmethod
    async def fetch_articles(self) -> list[Article]:
        """Fetch raw articles from the source."""
        ...

    @staticmethod
    def hash_url(url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()

    async def scrape(self) -> int:
        """Template method: fetch → hash → tag → deduplicate-insert."""
        try:
            articles = await self.fetch_articles()
        except Exception:
            logger.exception("Error fetching from %s", self.source_name)
            return 0

        db = get_db()
        saved = 0

        for article in articles:
            article.url_hash = self.hash_url(article.url)
            article.source_name = self.source_name
            article.source_type = self.source_type
            article.scraped_at = datetime.utcnow()
            tag_article(article)

            try:
                await db.articles.insert_one(article.model_dump())
                saved += 1
            except Exception:
                # Duplicate url_hash → skip silently
                pass

        logger.info("%s: saved %d / %d articles", self.source_name, saved, len(articles))
        return saved
