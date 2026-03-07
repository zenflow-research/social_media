import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

import httpx

from app.database import get_db
from app.models.parivesh_proposal import PariveshDocument, PariveshProposal

logger = logging.getLogger(__name__)

MIS_BASE = "https://parivesh.nic.in/mis"
API_BASE = "https://parivesh.nic.in/parivesh_api"

SEARCH_URL = f"{MIS_BASE}/trackYourProposal/advanceSearchData"
CAF_URL = f"{API_BASE}/proponentApplicant/getCafDataByProposalNo"
DOC_URL = f"{MIS_BASE}/documentdetails/getDocumentDetail"
HISTORY_URL = f"{API_BASE}/trackYourProposal/historyDataOnProposal"

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

REQUEST_DELAY = 2.0  # Be respectful to government servers
REQUEST_TIMEOUT = 60.0  # advanceSearchData can be slow

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://parivesh.nic.in/newupgrade/",
    "Origin": "https://parivesh.nic.in",
}


def _load_companies() -> list[dict]:
    path = _DATA_DIR / "companies_500cr.json"
    if not path.exists():
        path = _DATA_DIR / "nifty500_companies.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _build_search_names() -> list[dict]:
    """Build list of search queries from Nifty 500 companies.

    Uses full_name for search (more likely to match on Parivesh).
    Returns list of {nse_code, search_name, full_name}.
    """
    companies = _load_companies()
    results = []
    seen = set()
    for c in companies:
        nse = c.get("nse_code", "")
        if not nse:
            continue
        # Use full name but strip "Ltd" / "Limited" for better matching
        full = c.get("full_name", "") or c.get("company_name", "")
        search = full.replace(" Ltd", "").replace(" Limited", "").strip()
        if not search or search in seen:
            continue
        seen.add(search)
        results.append({
            "nse_code": nse,
            "search_name": search,
            "full_name": full,
        })
    return results


class PariveshScraper:
    """Scrapes Parivesh environmental clearance proposals for Nifty 500 companies."""

    def __init__(self):
        self.source_name = "parivesh"

    async def _search_company(
        self, client: httpx.AsyncClient, search_text: str
    ) -> list[dict]:
        """Search Parivesh advanceSearchData for a company name."""
        try:
            resp = await client.get(
                SEARCH_URL,
                params={"text": search_text},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "data" in data:
                return data["data"] if isinstance(data["data"], list) else []
            return []
        except httpx.TimeoutException:
            logger.warning("Search timeout for: %s", search_text)
            return []
        except Exception:
            logger.exception("Search failed for: %s", search_text)
            return []

    async def _get_proposal_details(
        self, client: httpx.AsyncClient, proposal_no: str
    ) -> dict | None:
        """Get full proposal details via getCafDataByProposalNo."""
        try:
            resp = await client.post(
                CAF_URL,
                params={"proposal_no": proposal_no},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            logger.exception("Failed to get details for: %s", proposal_no)
            return None

    async def _get_documents(
        self, client: httpx.AsyncClient, proposal_id: int
    ) -> dict | None:
        """Get document listing for a proposal."""
        try:
            resp = await client.post(
                DOC_URL,
                params={"id": str(proposal_id)},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            logger.exception("Failed to get documents for ID: %d", proposal_id)
            return None

    def _parse_search_result(
        self, item: dict, nse_symbol: str
    ) -> PariveshProposal | None:
        """Parse a single search result into a PariveshProposal."""
        proposal_no = item.get("proposalNo", "")
        if not proposal_no:
            return None

        # certificate_url can be in multiple fields
        cert = item.get("certificateUrl") or item.get("certificate_url") or ""

        return PariveshProposal(
            proposal_no=proposal_no,
            proposal_id=item.get("id"),
            project_name=item.get("projectName", ""),
            company_name=item.get("nameOfUserAgency", ""),
            nse_symbol=nse_symbol,
            state=item.get("state", ""),
            sector=item.get("sector", ""),
            category=item.get("category", ""),
            clearance_type=item.get("proposalType") or item.get("clearanceType", ""),
            proposal_status=item.get("proposalStatus", ""),
            date_of_submission=item.get("dateOfSubmission", ""),
            certificate_url=cert,
            single_window_number=item.get("singleWindowNumber", ""),
            url=f"https://parivesh.nic.in/newupgrade/#/trackYourProposal/proposal-details?proposalId={proposal_no}&proposal={item.get('id', '')}",
        )

    def _parse_documents(
        self, doc_data: dict, proposal_no: str, proposal_id: int | None
    ) -> list[PariveshDocument]:
        """Parse document listing response into PariveshDocument objects."""
        docs = []
        for category in ["documentDetails", "agendaDetails", "momDetails", "cafDetails"]:
            for item in doc_data.get(category, []) or []:
                uuid = item.get("dd_uuid", "")
                if not uuid:
                    continue
                docs.append(PariveshDocument(
                    proposal_no=proposal_no,
                    proposal_id=proposal_id,
                    doc_uuid=uuid,
                    doc_name=item.get("dd_document_name", ""),
                    doc_type=item.get("dd_type", ""),
                    category=category,
                ))
        return docs

    async def scrape_company(
        self, client: httpx.AsyncClient, nse_code: str, search_name: str
    ) -> int:
        """Search and save proposals for a single company. Returns count saved."""
        db = get_db()
        saved = 0

        results = await self._search_company(client, search_name)
        if not results:
            return 0

        logger.info("Parivesh: %s → %d results", search_name, len(results))

        for item in results:
            proposal = self._parse_search_result(item, nse_code)
            if not proposal:
                continue

            # Skip if already scraped
            existing = await db.parivesh_proposals.find_one(
                {"proposal_no": proposal.proposal_no}
            )
            if existing:
                # Update nse_symbol if not set (might have been found via different search)
                if not existing.get("nse_symbol") and nse_code:
                    await db.parivesh_proposals.update_one(
                        {"proposal_no": proposal.proposal_no},
                        {"$addToSet": {"matched_symbols": nse_code}},
                    )
                continue

            try:
                doc = proposal.model_dump()
                doc["matched_symbols"] = [nse_code]
                await db.parivesh_proposals.insert_one(doc)
                saved += 1
            except Exception:
                pass  # Duplicate

            await asyncio.sleep(0.5)

        return saved

    async def fetch_documents_for_proposal(
        self, client: httpx.AsyncClient, proposal_no: str, proposal_id: int
    ) -> int:
        """Fetch and save documents for a single proposal. Returns count saved."""
        db = get_db()

        doc_data = await self._get_documents(client, proposal_id)
        if not doc_data:
            return 0

        docs = self._parse_documents(doc_data, proposal_no, proposal_id)
        saved = 0
        for doc in docs:
            try:
                await db.parivesh_documents.update_one(
                    {"proposal_no": proposal_no, "doc_uuid": doc.doc_uuid},
                    {"$set": doc.model_dump()},
                    upsert=True,
                )
                saved += 1
            except Exception:
                logger.exception("Failed to save document %s", doc.doc_uuid)

        # Mark proposal as documents fetched
        await db.parivesh_proposals.update_one(
            {"proposal_no": proposal_no},
            {"$set": {"documents_fetched": True}},
        )

        return saved

    async def scrape_all(self, batch_size: int = 50) -> dict:
        """Search Parivesh for all Nifty 500 companies.

        Args:
            batch_size: Number of companies to process per run (0 = all).
        """
        db = get_db()
        companies = _build_search_names()
        total_saved = 0
        total_searched = 0
        errors = []

        # Track which companies we've already searched
        searched_key = "parivesh_searched_companies"
        searched_doc = await db.parivesh_meta.find_one({"_id": searched_key})
        already_searched = set(searched_doc.get("symbols", [])) if searched_doc else set()

        # Filter to unsearched companies
        pending = [c for c in companies if c["nse_code"] not in already_searched]
        if batch_size > 0:
            pending = pending[:batch_size]

        logger.info(
            "Parivesh scrape: %d/%d companies pending (batch=%d)",
            len(pending), len(companies), batch_size,
        )

        async with httpx.AsyncClient(headers=BROWSER_HEADERS) as client:
            for company in pending:
                nse = company["nse_code"]
                search = company["search_name"]

                try:
                    count = await self.scrape_company(client, nse, search)
                    total_saved += count
                    total_searched += 1

                    # Mark as searched
                    await db.parivesh_meta.update_one(
                        {"_id": searched_key},
                        {"$addToSet": {"symbols": nse}},
                        upsert=True,
                    )
                except Exception as e:
                    errors.append({"symbol": nse, "error": str(e)})
                    logger.exception("Failed to search %s", nse)

                await asyncio.sleep(REQUEST_DELAY)

        return {
            "total_saved": total_saved,
            "companies_searched": total_searched,
            "companies_remaining": len(companies) - len(already_searched) - total_searched,
            "errors": errors[:20],
        }

    async def fetch_pending_documents(self, limit: int = 50) -> dict:
        """Fetch documents for proposals that haven't had docs fetched yet."""
        db = get_db()
        cursor = db.parivesh_proposals.find(
            {"documents_fetched": {"$ne": True}, "proposal_id": {"$ne": None}},
            {"proposal_no": 1, "proposal_id": 1},
        ).limit(limit)

        total_docs = 0
        processed = 0
        errors = []

        async with httpx.AsyncClient(headers=BROWSER_HEADERS) as client:
            async for doc in cursor:
                proposal_no = doc["proposal_no"]
                proposal_id = doc.get("proposal_id")
                if not proposal_id:
                    continue

                try:
                    count = await self.fetch_documents_for_proposal(
                        client, proposal_no, proposal_id
                    )
                    total_docs += count
                    processed += 1
                except Exception as e:
                    errors.append({"proposal_no": proposal_no, "error": str(e)})

                await asyncio.sleep(REQUEST_DELAY)

        return {
            "proposals_processed": processed,
            "documents_saved": total_docs,
            "errors": errors[:20],
        }
