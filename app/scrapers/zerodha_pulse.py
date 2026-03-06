import logging
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from app.models.article import Article
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

PULSE_URL = "https://pulse.zerodha.com/"


class ZerodhaPulseScraper(BaseScraper):
    source_name = "zerodha_pulse"
    source_type = "web"

    async def fetch_articles(self) -> list[Article]:
        articles: list[Article] = []

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(
                PULSE_URL,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            )
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")

        for item in soup.select("li.box.item"):
            link_el = item.select_one("h2.title a") or item.select_one("a")
            if not link_el:
                continue

            url = link_el.get("href", "").strip()
            title = link_el.get_text(strip=True)
            if not url or not title:
                continue

            summary = ""
            desc_el = item.select_one(".desc")
            if desc_el:
                summary = desc_el.get_text(strip=True)[:500]

            published_at = None
            time_el = item.select_one("span.date") or item.select_one("time")
            if time_el:
                try:
                    published_at = datetime.fromisoformat(time_el.get("datetime", ""))
                except (ValueError, TypeError):
                    pass

            image_url = ""
            img_el = item.select_one("img")
            if img_el:
                image_url = img_el.get("src", "")

            source = ""
            src_el = item.select_one("span.feed")
            if src_el:
                source = src_el.get_text(strip=True)

            articles.append(
                Article(
                    url=url,
                    title=title,
                    summary=summary,
                    published_at=published_at,
                    image_url=image_url,
                    extra={"original_source": source},
                )
            )

        logger.info("Zerodha Pulse: fetched %d articles", len(articles))
        return articles
