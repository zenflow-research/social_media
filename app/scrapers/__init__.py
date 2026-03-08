from app.scrapers.zerodha_pulse import ZerodhaPulseScraper
from app.scrapers.twitter import TwitterScraper
from app.scrapers.linkedin import LinkedInScraper
from app.scrapers.rss_generic import RSSGenericScraper
from app.scrapers.substack import SubstackScraper

SCRAPERS = {
    "zerodha_pulse": ZerodhaPulseScraper,
    "twitter": TwitterScraper,
    "linkedin": LinkedInScraper,
    "rss": RSSGenericScraper,
    "substack": SubstackScraper,
}

# ET scraper has its own collection (et_articles) and doesn't follow BaseScraper.
# It's registered separately in the scheduler, like PIB and Parivesh.

# PIB scraper is not in SCRAPERS because it doesn't follow BaseScraper pattern.
# It's registered separately in the scheduler.
