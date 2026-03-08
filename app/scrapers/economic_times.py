import asyncio
import hashlib
import json
import logging
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from app.database import get_db
from app.models.et_article import ETArticle
from app.services.keyword_filter import tag_et_article

logger = logging.getLogger(__name__)

BASE_URL = "https://economictimes.indiatimes.com"

# (path, category, sub_category)
SECTIONS = [
    # ── Homepage ──
    ("", "headlines", ""),

    # ── Markets ──
    ("/markets", "markets", ""),
    ("/markets/stocks/news", "markets", "stocks-news"),
    ("/markets/stocks/earnings", "markets", "earnings"),
    ("/markets/stocks/recos", "markets", "recos"),
    ("/markets/ipo", "markets", "ipo"),
    ("/markets/expert-view", "markets", "expert-view"),
    ("/markets/commodities", "markets", "commodities"),
    ("/markets/forex", "markets", "forex"),
    ("/markets/us-stocks", "markets", "us-stocks"),
    ("/markets/cryptocurrency", "markets", "crypto"),
    ("/markets/bonds", "markets", "bonds"),
    ("/markets/market-moguls", "markets", "market-moguls"),

    # ── Economy ──
    ("/news/economy", "economy", ""),
    ("/news/economy/policy", "economy", "policy"),
    ("/news/economy/finance", "economy", "finance"),
    ("/news/economy/indicators", "economy", "indicators"),
    ("/news/economy/infrastructure", "economy", "infrastructure"),
    ("/news/economy/agriculture", "economy", "agriculture"),
    ("/news/economy/foreign-trade", "economy", "foreign-trade"),

    # ── Industry ──
    ("/industry", "industry", ""),
    ("/industry/auto", "industry", "auto"),
    ("/industry/banking/finance", "industry", "banking-finance"),
    ("/industry/cons-products", "industry", "consumer-products"),
    ("/industry/energy", "industry", "energy"),
    ("/industry/renewables", "industry", "renewables"),
    ("/industry/indl-goods/svs", "industry", "industrial-goods"),
    ("/industry/healthcare/biotech", "industry", "healthcare"),
    ("/industry/services", "industry", "services"),
    ("/industry/media/entertainment", "industry", "media"),
    ("/industry/transportation", "industry", "transportation"),
    ("/industry/telecom", "industry", "telecom"),

    # ── Other sections ──
    ("/tech", "tech", ""),
    ("/small-biz", "sme", ""),
    ("/personal-finance", "wealth", ""),
    ("/mutual-funds", "mutual-funds", ""),
    ("/opinion", "opinion", ""),
    ("/news/politics", "politics", ""),
    ("/news/defence", "defence", ""),
    ("/news/international", "international", ""),
    ("/news/company/corporate-trends", "corporate", ""),
    ("/jobs", "careers", ""),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


class EconomicTimesScraper:
    """Scrapes Economic Times across all sections into the et_articles collection."""

    async def scrape(self) -> dict:
        """Scrape all sections → deduplicate → save to et_articles. Returns stats."""
        articles: list[ETArticle] = []
        seen_urls: set[str] = set()
        errors: list[str] = []

        async with httpx.AsyncClient(
            timeout=30, follow_redirects=True, headers=HEADERS
        ) as client:
            for path, category, sub_category in SECTIONS:
                try:
                    found = await self._scrape_section(
                        client, path, category, sub_category, seen_urls
                    )
                    articles.extend(found)
                except Exception as e:
                    logger.exception("ET: error scraping %s%s", BASE_URL, path)
                    errors.append(f"{category}/{sub_category}: {e}")

        logger.info(
            "ET: fetched %d articles from %d sections (%d errors)",
            len(articles), len(SECTIONS), len(errors),
        )

        # Save to MongoDB
        db = get_db()
        saved = 0
        for article in articles:
            article.url_hash = hashlib.sha256(article.url.encode()).hexdigest()
            article.scraped_at = datetime.utcnow()
            tag_et_article(article)
            try:
                await db.et_articles.insert_one(article.model_dump())
                saved += 1
            except Exception:
                pass  # duplicate url_hash → skip

        logger.info("ET: saved %d / %d articles", saved, len(articles))
        return {
            "saved": saved,
            "fetched": len(articles),
            "sections": len(SECTIONS),
            "errors": errors,
        }

    async def _scrape_section(
        self,
        client: httpx.AsyncClient,
        path: str,
        category: str,
        sub_category: str,
        seen_urls: set[str],
    ) -> list[ETArticle]:
        url = f"{BASE_URL}{path}"
        resp = await client.get(url)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        articles: list[ETArticle] = []

        # Collect story containers using multiple selector patterns
        story_elements = []
        for selector in [
            "div.eachStory",
            "div.story-box",
            "div.clr.flt.topnews",
            "div.story_list div.eachStory",
            "div.cardHolder",
        ]:
            story_elements.extend(soup.select(selector))

        # Also grab standalone article links
        for selector in [
            "div.top-stories a[href*='/articleshow/']",
            "div.data_cnt a[href*='/articleshow/']",
            "a.wrapLines[href*='/articleshow/']",
        ]:
            story_elements.extend(soup.select(selector))

        # Fallback: all articleshow links on page
        if not story_elements:
            story_elements = soup.select("a[href*='/articleshow/']")

        for elem in story_elements:
            try:
                article = self._parse_element(elem, category, sub_category, seen_urls)
                if article:
                    articles.append(article)
            except Exception:
                continue

        logger.debug("ET [%s/%s]: found %d articles", category, sub_category, len(articles))
        return articles

    def _parse_element(
        self,
        elem,
        category: str,
        sub_category: str,
        seen_urls: set[str],
    ) -> ETArticle | None:
        # Find the link
        if elem.name == "a":
            link_el = elem
        else:
            link_el = (
                elem.select_one("a[href*='/articleshow/']")
                or elem.select_one("a[href]")
            )

        if not link_el:
            return None

        href = link_el.get("href", "").strip()
        if not href or "/articleshow/" not in href:
            return None

        # Absolute URL
        if href.startswith("/"):
            href = f"{BASE_URL}{href}"
        elif not href.startswith("http"):
            return None

        # Strip query params for dedup
        clean_url = href.split("?")[0]
        if clean_url in seen_urls:
            return None
        seen_urls.add(clean_url)

        # Title
        title = link_el.get_text(strip=True)
        if not title or len(title) < 10:
            h_el = elem.select_one("h2, h3, h4")
            if h_el:
                title = h_el.get_text(strip=True)
        if not title or len(title) < 10:
            return None

        # Summary
        summary = ""
        desc_el = (
            elem.select_one("p")
            or elem.select_one(".synopis")
            or elem.select_one(".disc")
        )
        if desc_el:
            summary = desc_el.get_text(strip=True)[:500]

        # Image
        image_url = ""
        img_el = elem.select_one("img")
        if img_el:
            image_url = img_el.get("data-src", "") or img_el.get("src", "")
            if image_url and image_url.startswith("//"):
                image_url = "https:" + image_url

        # Time
        published_at = None
        time_el = elem.select_one("time")
        if time_el:
            try:
                published_at = datetime.fromisoformat(time_el.get("datetime", ""))
            except (ValueError, TypeError):
                pass

        return ETArticle(
            url=clean_url,
            title=title,
            summary=summary,
            category=category,
            sub_category=sub_category,
            image_url=image_url,
            published_at=published_at,
        )

    # ── Full-text fetching ─────────────────────────────────────────

    async def fetch_article_texts(self, limit: int = 50) -> dict:
        """Fetch full article text for articles that don't have it yet."""
        db = get_db()
        cursor = (
            db.et_articles.find(
                {"text_fetched": {"$ne": True}},
                {"_id": 0, "url": 1, "url_hash": 1},
            )
            .sort("scraped_at", -1)
            .limit(limit)
        )
        pending = await cursor.to_list(length=limit)
        if not pending:
            return {"fetched": 0, "failed": 0, "total_pending": 0}

        total_pending = await db.et_articles.count_documents(
            {"text_fetched": {"$ne": True}}
        )

        fetched = 0
        failed = 0

        async with httpx.AsyncClient(
            timeout=30, follow_redirects=True, headers=HEADERS
        ) as client:
            for doc in pending:
                try:
                    result = await self._fetch_single_article(client, doc["url"])
                    if result:
                        update: dict = {"text_fetched": True, **result}
                        await db.et_articles.update_one(
                            {"url_hash": doc["url_hash"]},
                            {"$set": update},
                        )
                        fetched += 1
                    else:
                        # Mark as fetched even if empty to avoid retrying
                        await db.et_articles.update_one(
                            {"url_hash": doc["url_hash"]},
                            {"$set": {"text_fetched": True}},
                        )
                        failed += 1
                except Exception:
                    logger.exception("ET: error fetching text for %s", doc["url"])
                    failed += 1

                # Rate limit: 0.5s between requests
                await asyncio.sleep(0.5)

        logger.info("ET text fetch: %d fetched, %d failed", fetched, failed)
        return {
            "fetched": fetched,
            "failed": failed,
            "total_pending": total_pending - fetched - failed,
        }

    async def _fetch_single_article(
        self, client: httpx.AsyncClient, url: str
    ) -> dict | None:
        """Fetch a single article page and extract full text via JSON-LD."""
        resp = await client.get(url)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")

        # Prefer JSON-LD structured data (clean, no HTML noise)
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and "articleBody" in data:
                    result: dict = {"full_text": data["articleBody"]}
                    if data.get("author"):
                        author = data["author"]
                        if isinstance(author, dict):
                            result["author"] = author.get("name", "")
                        elif isinstance(author, list) and author:
                            result["author"] = author[0].get("name", "")
                    if data.get("description"):
                        result["summary"] = data["description"]
                    if data.get("datePublished"):
                        try:
                            result["published_at"] = datetime.fromisoformat(
                                data["datePublished"]
                            )
                        except (ValueError, TypeError):
                            pass
                    if data.get("image"):
                        img = data["image"]
                        if isinstance(img, list):
                            img = img[0] if img else ""
                        if isinstance(img, dict):
                            img = img.get("url", "")
                        if img:
                            result["image_url"] = img
                    return result
            except (json.JSONDecodeError, TypeError):
                continue

        # Fallback: extract from artText div
        art_div = soup.select_one("div.artText")
        if art_div:
            text = art_div.get_text(separator="\n", strip=True)
            if len(text) > 50:
                return {"full_text": text}

        return None
