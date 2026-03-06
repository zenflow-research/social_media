import asyncio
import logging
import re
from datetime import datetime, timedelta

import httpx
from bs4 import BeautifulSoup

from app.database import get_db
from app.models.pib_release import PIBRelease

logger = logging.getLogger(__name__)

BASE_URL = "https://www.pib.gov.in"
LISTING_URL = f"{BASE_URL}/allRel.aspx"
DETAIL_URL = f"{BASE_URL}/PressReleasePage.aspx"

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Titles matching these patterns are not market-relevant
SKIP_PATTERNS = re.compile(
    r"(photo\s*gallery|greeting|condolence|obituary|"
    r"birth\s*anniversary|foundation\s*day\s*celebration|"
    r"mann\s*ki\s*baat|yoga\s*day)",
    re.IGNORECASE,
)

# Rate limit: delay between requests in seconds
REQUEST_DELAY = 1.0
REQUEST_TIMEOUT = 30.0


class PIBScraper:
    """Scrapes press releases from Press Information Bureau (pib.gov.in)."""

    def __init__(self):
        self.source_name = "pib"
        self.source_type = "government"

    async def _fetch_page(self, client: httpx.AsyncClient, url: str, params: dict | None = None) -> str | None:
        try:
            resp = await client.get(url, params=params, timeout=REQUEST_TIMEOUT,
                                    follow_redirects=True)
            resp.raise_for_status()
            return resp.text
        except Exception:
            logger.exception("Failed to fetch %s", url)
            return None

    def _extract_prids_from_listing(self, html: str) -> list[dict]:
        """Extract PRID and ministry from the listing page."""
        soup = BeautifulSoup(html, "lxml")
        releases = []
        seen_prids = set()

        # PIB listing has links like PressReleasePage.aspx?PRID=1234567
        for link in soup.find_all("a", href=True):
            href = link["href"]
            match = re.search(r"PRID=(\d+)", href)
            if not match:
                continue
            prid = int(match.group(1))
            if prid in seen_prids:
                continue
            seen_prids.add(prid)

            title = link.get_text(strip=True)
            if not title:
                continue

            # Try to find ministry from parent elements
            ministry = ""
            parent = link.find_parent("div")
            if parent:
                # Ministry names are often in header elements above the link
                header = parent.find_previous(["h3", "h4", "h5", "strong", "b"])
                if header:
                    ministry = header.get_text(strip=True)

            releases.append({
                "prid": prid,
                "title": title,
                "ministry": ministry,
            })

        return releases

    def _parse_detail_page(self, html: str, prid: int) -> PIBRelease | None:
        """Parse a single press release detail page."""
        soup = BeautifulSoup(html, "lxml")

        # Title
        title_el = soup.find("h2") or soup.find("h3")
        title = title_el.get_text(strip=True) if title_el else ""

        # Ministry - usually in a div with specific class or above the title
        ministry = ""
        ministry_el = soup.find("div", class_=re.compile(r"ministry|department", re.I))
        if ministry_el:
            ministry = ministry_el.get_text(strip=True)
        if not ministry:
            # Try the first bold/strong text before content
            for tag in soup.find_all(["strong", "b"]):
                text = tag.get_text(strip=True)
                if "Ministry" in text or "Department" in text or "Commission" in text:
                    ministry = text
                    break

        # Date - look for date pattern in the page
        published_at = None
        date_el = soup.find(string=re.compile(r"\d{1,2}\s+\w+\s+\d{4}"))
        if date_el:
            date_match = re.search(r"(\d{1,2}\s+\w{3,}\s+\d{4})", date_el)
            if date_match:
                try:
                    published_at = datetime.strptime(date_match.group(1).strip(), "%d %b %Y")
                except ValueError:
                    try:
                        published_at = datetime.strptime(date_match.group(1).strip(), "%d %B %Y")
                    except ValueError:
                        pass

        # Full text content
        content_div = soup.find("div", id=re.compile(r"content", re.I))
        if not content_div:
            content_div = soup.find("div", class_=re.compile(r"content|innercontent", re.I))
        if not content_div:
            # Fallback: get the main text area
            content_div = soup.find("div", id="PressReleaseContent") or soup.body

        full_text = ""
        if content_div:
            # Remove script/style tags
            for tag in content_div.find_all(["script", "style"]):
                tag.decompose()
            full_text = content_div.get_text(separator="\n", strip=True)

        if not title and not full_text:
            return None

        return PIBRelease(
            prid=prid,
            title=title,
            ministry=ministry,
            published_at=published_at,
            full_text=full_text[:50000],  # Cap at 50k chars
            url=f"{DETAIL_URL}?PRID={prid}",
        )

    async def scrape_latest(self) -> int:
        """Scrape the latest releases from the default listing page."""
        db = get_db()
        saved = 0

        async with httpx.AsyncClient(
            headers=BROWSER_HEADERS,
            follow_redirects=True,
        ) as client:
            html = await self._fetch_page(client, LISTING_URL, {"reg": "3", "lang": "1"})
            if not html:
                return 0

            entries = self._extract_prids_from_listing(html)
            logger.info("PIB listing: found %d releases", len(entries))

            for entry in entries:
                prid = entry["prid"]

                # Skip if already scraped
                if await db.pib_releases.find_one({"prid": prid}):
                    continue

                # Skip non-market-relevant titles
                if SKIP_PATTERNS.search(entry.get("title", "")):
                    logger.debug("Skipping non-relevant: %s", entry["title"])
                    continue

                await asyncio.sleep(REQUEST_DELAY)

                detail_html = await self._fetch_page(client, DETAIL_URL, {"PRID": str(prid)})
                if not detail_html:
                    continue

                release = self._parse_detail_page(detail_html, prid)
                if not release:
                    continue

                # Use ministry from listing if detail page didn't find one
                if not release.ministry and entry.get("ministry"):
                    release.ministry = entry["ministry"]

                try:
                    await db.pib_releases.insert_one(release.model_dump())
                    saved += 1
                except Exception:
                    pass  # Duplicate prid

        logger.info("PIB: saved %d new releases", saved)
        return saved

    def _extract_hidden_fields(self, html: str) -> dict:
        """Extract ALL hidden input fields needed for ASP.NET postback."""
        soup = BeautifulSoup(html, "lxml")
        fields = {}
        for el in soup.find_all("input", {"type": "hidden"}):
            name = el.get("name", "")
            if name:
                fields[name] = el.get("value", "")
        return fields

    async def _fetch_date_page(
        self, client: httpx.AsyncClient, day: int, month: int, year: int,
        hidden_fields: dict,
    ) -> str | None:
        """Fetch PIB listing for a specific date via ASP.NET postback.

        Key: POST to URL with ?reg=3&lang=1 query params to avoid redirect
        to Hindi version. Include ALL hidden fields from the page.
        """
        form_data = {
            **hidden_fields,
            "ctl00$ContentPlaceHolder1$ddlMinistry": "0",  # All ministries
            "ctl00$ContentPlaceHolder1$ddlday": str(day),
            "ctl00$ContentPlaceHolder1$ddlMonth": str(month),
            "ctl00$ContentPlaceHolder1$ddlYear": str(year),
            "ctl00$ContentPlaceHolder1$hydregionid": "3",
            "ctl00$ContentPlaceHolder1$hydLangid": "1",
            "ctl00$Bar1$ddlregion": "3",
            "ctl00$Bar1$ddlLang": "1",
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlday",
            "__EVENTARGUMENT": "",
        }
        try:
            resp = await client.post(
                f"{LISTING_URL}?reg=3&lang=1",
                data=form_data,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.text
        except Exception:
            logger.exception("Failed to fetch date page %d/%d/%d", day, month, year)
            return None

    async def backfill(self, days: int = 365) -> dict:
        """Backfill historical PIB releases using ASP.NET postback date filtering."""
        db = get_db()
        total_saved = 0
        total_skipped = 0
        total_found = 0
        errors = []
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        current_date = end_date

        async with httpx.AsyncClient(
            headers={**BROWSER_HEADERS,
                     "Referer": f"{LISTING_URL}?reg=3&lang=1",
                     "Origin": BASE_URL},
            follow_redirects=False,
        ) as client:
            # First, get the initial page (need follow_redirects for GET)
            try:
                resp = await client.get(f"{LISTING_URL}?reg=3&lang=1",
                                        follow_redirects=True, timeout=REQUEST_TIMEOUT)
                initial_html = resp.text
            except Exception:
                return {"error": "Failed to load initial PIB page"}

            hidden_fields = self._extract_hidden_fields(initial_html)
            if not hidden_fields.get("__VIEWSTATE"):
                return {"error": "Failed to extract ASP.NET form fields"}

            while current_date >= start_date:
                day = current_date.day
                month = current_date.month
                year = current_date.year

                logger.info("Backfilling PIB: %d/%d/%d", day, month, year)

                html = await self._fetch_date_page(client, day, month, year, hidden_fields)
                if html:
                    # Update hidden fields from response for next request
                    new_fields = self._extract_hidden_fields(html)
                    if new_fields.get("__VIEWSTATE"):
                        hidden_fields = new_fields

                    entries = self._extract_prids_from_listing(html)
                    total_found += len(entries)
                    batch_saved = 0

                    for entry in entries:
                        prid = entry["prid"]
                        if await db.pib_releases.find_one({"prid": prid}):
                            total_skipped += 1
                            continue

                        if SKIP_PATTERNS.search(entry.get("title", "")):
                            continue

                        await asyncio.sleep(REQUEST_DELAY)

                        detail_html = await self._fetch_page(client, DETAIL_URL, {"PRID": str(prid)})
                        if not detail_html:
                            continue

                        release = self._parse_detail_page(detail_html, prid)
                        if not release:
                            continue

                        if not release.ministry and entry.get("ministry"):
                            release.ministry = entry["ministry"]
                        if not release.published_at:
                            release.published_at = current_date

                        try:
                            await db.pib_releases.insert_one(release.model_dump())
                            batch_saved += 1
                            total_saved += 1
                        except Exception:
                            total_skipped += 1

                    logger.info(
                        "Backfill %d/%d/%d: found %d, saved %d, total: %d",
                        day, month, year, len(entries), batch_saved, total_saved,
                    )
                else:
                    errors.append(f"{day}/{month}/{year}")

                current_date -= timedelta(days=1)
                await asyncio.sleep(REQUEST_DELAY)

        return {
            "total_saved": total_saved,
            "total_skipped": total_skipped,
            "total_found": total_found,
            "errors": errors[:50],
            "date_range": f"{start_date.date()} to {end_date.date()}",
            "days_processed": days,
        }
