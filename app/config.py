from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "newsscrapper"

    scrape_interval_zerodha: int = 15
    scrape_interval_twitter: int = 30
    scrape_interval_rss: int = 20
    scrape_interval_linkedin: int = 0  # 0 = disabled
    scrape_interval_substack: int = 30
    scrape_interval_pib: int = 60  # PIB: every 60 min
    pib_analyze_interval: int = 120  # Run LLM analysis every 2 hours
    pib_analyze_batch_size: int = 20  # Max releases to analyze per batch

    keywords: str = "nifty,sensex,rbi,fed,inflation,earnings,ipo,stock,market,crypto,bitcoin,gold,rupee,dollar,gdp,budget,sebi,mutual fund,etf,dividend"
    rss_feeds: str = "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms,https://www.livemint.com/rss/markets"
    twitter_accounts: str = "ZeprodhaPulse,monaboradakker,MarketAnalysis"
    linkedin_cookie: str = ""
    substack_urls: str = "https://zennivesh.substack.com/,https://91capital.substack.com/"

    @property
    def keyword_list(self) -> list[str]:
        return [k.strip().lower() for k in self.keywords.split(",") if k.strip()]

    @property
    def rss_feed_list(self) -> list[str]:
        return [u.strip() for u in self.rss_feeds.split(",") if u.strip()]

    @property
    def substack_list(self) -> list[str]:
        return [u.strip() for u in self.substack_urls.split(",") if u.strip()]

    @property
    def twitter_account_list(self) -> list[str]:
        return [a.strip() for a in self.twitter_accounts.split(",") if a.strip()]

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
