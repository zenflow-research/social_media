import asyncio
import logging

from app.config import settings
from app.models.article import Article
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class TwitterScraper(BaseScraper):
    source_name = "twitter"
    source_type = "twitter"

    async def fetch_articles(self) -> list[Article]:
        from ntscraper import Nitter

        articles: list[Article] = []
        scraper = Nitter()

        for account in settings.twitter_account_list:
            try:
                tweets = await asyncio.to_thread(
                    scraper.get_tweets, account, mode="user", number=10
                )
            except Exception:
                logger.exception("Failed to scrape Twitter account: %s", account)
                continue

            for tweet in tweets.get("tweets", []):
                text = tweet.get("text", "")
                link = tweet.get("link", "")
                if not link:
                    continue

                # Ensure full URL
                if link.startswith("/"):
                    link = f"https://nitter.net{link}"

                articles.append(
                    Article(
                        url=link,
                        title=text[:120],
                        summary=text,
                        raw_content=text,
                        extra={
                            "author": account,
                            "stats": tweet.get("stats", {}),
                        },
                    )
                )

        logger.info("Twitter: fetched %d tweets", len(articles))
        return articles
