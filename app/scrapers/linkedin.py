import logging

from app.config import settings
from app.models.article import Article
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class LinkedInScraper(BaseScraper):
    """LinkedIn scraper skeleton — requires Playwright + li_at cookie auth.

    LinkedIn aggressively blocks scraping. This is a placeholder that can be
    extended once a valid li_at session cookie is configured in .env.
    """

    source_name = "linkedin"
    source_type = "linkedin"

    async def fetch_articles(self) -> list[Article]:
        if not settings.linkedin_cookie:
            logger.warning("LinkedIn scraper skipped: no LINKEDIN_COOKIE set in .env")
            return []

        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.error("Playwright not installed. Run: playwright install chromium")
            return []

        articles: list[Article] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context()

            # Set LinkedIn session cookie
            await context.add_cookies([{
                "name": "li_at",
                "value": settings.linkedin_cookie,
                "domain": ".linkedin.com",
                "path": "/",
            }])

            page = await context.new_page()

            try:
                await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)

                posts = await page.query_selector_all("div.feed-shared-update-v2")
                for post in posts[:10]:
                    text_el = await post.query_selector("span.break-words")
                    text = await text_el.inner_text() if text_el else ""

                    link_el = await post.query_selector("a.app-aware-link")
                    link = await link_el.get_attribute("href") if link_el else ""

                    if text and link:
                        articles.append(
                            Article(
                                url=link,
                                title=text[:120],
                                summary=text[:500],
                                raw_content=text,
                            )
                        )
            except Exception:
                logger.exception("LinkedIn scraping failed")
            finally:
                await browser.close()

        logger.info("LinkedIn: fetched %d posts", len(articles))
        return articles
