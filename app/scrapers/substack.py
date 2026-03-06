import asyncio
import logging
from datetime import datetime
from email.utils import parsedate_to_datetime

import feedparser
import httpx
from bs4 import BeautifulSoup

from app.config import settings
from app.models.article import Article
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class SubstackScraper(BaseScraper):
    source_name = "substack"
    source_type = "newsletter"

    async def _fetch_full_content(self, url: str) -> str:
        """Fetch full post content from the Substack article page."""
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                resp = await client.get(url)
                resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            body = soup.select_one("div.body.markup")
            if body:
                return body.get_text(separator="\n", strip=True)
        except Exception:
            logger.debug("Could not fetch full content for %s", url)
        return ""

    async def fetch_articles(self) -> list[Article]:
        articles: list[Article] = []

        for base_url in settings.substack_list:
            feed_url = base_url.rstrip("/") + "/feed"
            try:
                feed = await asyncio.to_thread(feedparser.parse, feed_url)
            except Exception:
                logger.exception("Failed to parse Substack feed: %s", feed_url)
                continue

            newsletter_name = feed.feed.get("title", base_url)

            for entry in feed.entries:
                url = entry.get("link", "")
                if not url:
                    continue

                title = entry.get("title", "")
                summary = entry.get("summary", "")[:500]
                author = entry.get("author", "")

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

                # Get full content from the entry itself (Substack RSS includes it)
                raw_content = ""
                if entry.get("content"):
                    raw_html = entry.content[0].get("value", "")
                    soup = BeautifulSoup(raw_html, "html.parser")
                    raw_content = soup.get_text(separator="\n", strip=True)

                articles.append(
                    Article(
                        url=url,
                        title=title,
                        summary=summary,
                        published_at=published_at,
                        image_url=image_url,
                        raw_content=raw_content,
                        extra={
                            "newsletter": newsletter_name,
                            "author": author,
                            "feed_url": feed_url,
                        },
                    )
                )

        logger.info(
            "Substack: fetched %d articles from %d newsletters",
            len(articles),
            len(settings.substack_list),
        )
        return articles
