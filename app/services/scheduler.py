import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.config import settings
from app.scrapers import SCRAPERS

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def _run_scraper(name: str) -> None:
    scraper_cls = SCRAPERS.get(name)
    if not scraper_cls:
        logger.error("Unknown scraper: %s", name)
        return
    scraper = scraper_cls()
    count = await scraper.scrape()
    logger.info("Scheduled %s: saved %d articles", name, count)


async def _run_pib_scraper() -> None:
    from app.scrapers.pib import PIBScraper
    scraper = PIBScraper()
    count = await scraper.scrape_latest()
    logger.info("Scheduled PIB scraper: saved %d releases", count)


async def _run_pib_analyzer() -> None:
    from app.services.pib_analyzer import analyze_pending
    results = await analyze_pending(limit=settings.pib_analyze_batch_size)
    logger.info(
        "Scheduled PIB analyzer: analyzed=%d, failed=%d",
        results["analyzed"], results["failed"],
    )


def start_scheduler() -> None:
    """Register scrape jobs based on config intervals and start the scheduler."""

    interval_map = {
        "zerodha_pulse": settings.scrape_interval_zerodha,
        "rss": settings.scrape_interval_rss,
        "twitter": settings.scrape_interval_twitter,
        "linkedin": settings.scrape_interval_linkedin,
        "substack": settings.scrape_interval_substack,
    }

    for source, minutes in interval_map.items():
        if minutes > 0:
            scheduler.add_job(
                _run_scraper,
                "interval",
                minutes=minutes,
                args=[source],
                id=f"scrape_{source}",
                replace_existing=True,
            )
            logger.info("Scheduled %s every %d min", source, minutes)
        else:
            logger.info("Skipping %s (interval=0, disabled)", source)

    # PIB scraper
    if settings.scrape_interval_pib > 0:
        scheduler.add_job(
            _run_pib_scraper,
            "interval",
            minutes=settings.scrape_interval_pib,
            id="scrape_pib",
            replace_existing=True,
        )
        logger.info("Scheduled PIB scraper every %d min", settings.scrape_interval_pib)

    # PIB LLM analyzer
    if settings.pib_analyze_interval > 0:
        scheduler.add_job(
            _run_pib_analyzer,
            "interval",
            minutes=settings.pib_analyze_interval,
            id="analyze_pib",
            replace_existing=True,
        )
        logger.info("Scheduled PIB analyzer every %d min", settings.pib_analyze_interval)

    scheduler.start()
    logger.info("Scheduler started")


def stop_scheduler() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
