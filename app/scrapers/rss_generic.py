import asyncio
import logging
from datetime import datetime
from email.utils import parsedate_to_datetime

import feedparser

from app.config import settings
from app.models.article import Article
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class RSSGenericScraper(BaseScraper):
    source_name = "rss"
    source_type = "rss"

    async def fetch_articles(self) -> list[Article]:
        articles: list[Article] = []

        for feed_url in settings.rss_feed_list:
            try:
                feed = await asyncio.to_thread(feedparser.parse, feed_url)
            except Exception:
                logger.exception("Failed to parse RSS feed: %s", feed_url)
                continue

            feed_title = feed.feed.get("title", feed_url)

            for entry in feed.entries:
                url = entry.get("link", "")
                if not url:
                    continue

                title = entry.get("title", "")
                summary = entry.get("summary", "")[:500]

                published_at = None
                pub_str = entry.get("published") or entry.get("updated")
                if pub_str:
                    try:
                        published_at = parsedate_to_datetime(pub_str)
                    except (ValueError, TypeError):
                        try:
                            published_at = datetime.fromisoformat(pub_str)
                        except (ValueError, TypeError):
                            pass

                image_url = ""
                if entry.get("media_content"):
                    image_url = entry.media_content[0].get("url", "")
                elif entry.get("enclosures"):
                    image_url = entry.enclosures[0].get("href", "")

                articles.append(
                    Article(
                        url=url,
                        title=title,
                        summary=summary,
                        published_at=published_at,
                        image_url=image_url,
                        extra={"feed_title": feed_title, "feed_url": feed_url},
                    )
                )

        logger.info("RSS: fetched %d articles from %d feeds", len(articles), len(settings.rss_feed_list))
        return articles
