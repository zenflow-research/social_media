import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db
from app.scrapers import SCRAPERS

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


# ── HTML Dashboard ──────────────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


# ── REST API ────────────────────────────────────────────────────────

@router.get("/api/articles")
async def list_articles(
    source: str = "",
    tag: str = "",
    q: str = "",
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    db = get_db()
    query_filter: dict = {}

    if source:
        query_filter["source_name"] = source
    if tag:
        query_filter["tags"] = tag
    if q:
        query_filter["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"summary": {"$regex": q, "$options": "i"}},
        ]

    skip = (page - 1) * per_page
    total = await db.articles.count_documents(query_filter)
    cursor = db.articles.find(query_filter, {"_id": 0}).sort("scraped_at", -1).skip(skip).limit(per_page)
    articles = await cursor.to_list(length=per_page)

    return {"total": total, "page": page, "per_page": per_page, "articles": articles}


@router.get("/api/articles/{url_hash}")
async def get_article(url_hash: str):
    db = get_db()
    article = await db.articles.find_one({"url_hash": url_hash}, {"_id": 0})
    if not article:
        raise HTTPException(status_code=404, detail="Article not found")
    return article


@router.get("/api/stats")
async def get_stats():
    db = get_db()
    total = await db.articles.count_documents({})

    # Count by source
    pipeline_source = [
        {"$group": {"_id": "$source_name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    by_source = {doc["_id"]: doc["count"] async for doc in db.articles.aggregate(pipeline_source)}

    # Top tags
    pipeline_tags = [
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20},
    ]
    top_tags = {doc["_id"]: doc["count"] async for doc in db.articles.aggregate(pipeline_tags)}

    # Latest scrape time
    latest = await db.articles.find_one(sort=[("scraped_at", -1)])
    last_scraped = latest["scraped_at"] if latest else None

    return {
        "total_articles": total,
        "by_source": by_source,
        "top_tags": top_tags,
        "last_scraped": last_scraped,
    }


@router.post("/api/scrape/{source}")
async def trigger_scrape(source: str):
    scraper_cls = SCRAPERS.get(source)
    if not scraper_cls:
        return {"error": f"Unknown source: {source}. Available: {list(SCRAPERS.keys())}"}

    scraper = scraper_cls()
    count = await scraper.scrape()
    return {"source": source, "saved": count, "timestamp": datetime.utcnow().isoformat()}


# ── Economic Times Endpoints ───────────────────────────────────────

@router.get("/api/et/articles")
async def list_et_articles(
    category: str = "",
    sub_category: str = "",
    tag: str = "",
    q: str = "",
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """List Economic Times articles with filters."""
    db = get_db()
    query_filter: dict = {}

    if category:
        query_filter["category"] = category
    if sub_category:
        query_filter["sub_category"] = sub_category
    if tag:
        query_filter["tags"] = tag
    if q:
        query_filter["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"summary": {"$regex": q, "$options": "i"}},
        ]

    skip = (page - 1) * per_page
    total = await db.et_articles.count_documents(query_filter)
    cursor = (
        db.et_articles.find(query_filter, {"_id": 0})
        .sort("scraped_at", -1)
        .skip(skip)
        .limit(per_page)
    )
    articles = await cursor.to_list(length=per_page)

    return {"total": total, "page": page, "per_page": per_page, "articles": articles}


@router.get("/api/et/stats")
async def et_stats():
    """Economic Times collection statistics."""
    db = get_db()
    total = await db.et_articles.count_documents({})

    # By category
    pipeline_cat = [
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    by_category = {
        doc["_id"]: doc["count"]
        async for doc in db.et_articles.aggregate(pipeline_cat)
        if doc["_id"]
    }

    # By sub_category (only non-empty)
    pipeline_sub = [
        {"$match": {"sub_category": {"$ne": ""}}},
        {"$group": {"_id": {"cat": "$category", "sub": "$sub_category"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 30},
    ]
    by_sub_category = {
        f"{doc['_id']['cat']}/{doc['_id']['sub']}": doc["count"]
        async for doc in db.et_articles.aggregate(pipeline_sub)
    }

    # Top tags
    pipeline_tags = [
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20},
    ]
    top_tags = {
        doc["_id"]: doc["count"]
        async for doc in db.et_articles.aggregate(pipeline_tags)
        if doc["_id"]
    }

    # Latest scrape
    latest = await db.et_articles.find_one(sort=[("scraped_at", -1)])
    last_scraped = latest["scraped_at"] if latest else None

    return {
        "total_articles": total,
        "by_category": by_category,
        "by_sub_category": by_sub_category,
        "top_tags": top_tags,
        "last_scraped": last_scraped,
    }


@router.post("/api/et/scrape")
async def trigger_et_scrape():
    """Manually trigger Economic Times scraping."""
    from app.scrapers.economic_times import EconomicTimesScraper
    scraper = EconomicTimesScraper()
    result = await scraper.scrape()
    return {
        "source": "economic_times",
        "saved": result["saved"],
        "fetched": result["fetched"],
        "sections": result["sections"],
        "errors": result["errors"],
        "timestamp": datetime.utcnow().isoformat(),
    }


# ── PIB Endpoints ──────────────────────────────────────────────────

@router.get("/api/pib/releases")
async def list_pib_releases(
    ministry: str = "",
    q: str = "",
    analyzed: str = "",  # "true" / "false" / "" (all)
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    db = get_db()
    query_filter: dict = {}

    if ministry:
        query_filter["ministry"] = {"$regex": ministry, "$options": "i"}
    if q:
        query_filter["$text"] = {"$search": q}
    if analyzed == "true":
        query_filter["analyzed"] = True
    elif analyzed == "false":
        query_filter["analyzed"] = {"$ne": True}

    skip = (page - 1) * per_page
    total = await db.pib_releases.count_documents(query_filter)
    cursor = (
        db.pib_releases.find(query_filter, {"_id": 0, "full_text": 0})
        .sort("published_at", -1)
        .skip(skip)
        .limit(per_page)
    )
    releases = await cursor.to_list(length=per_page)

    return {"total": total, "page": page, "per_page": per_page, "releases": releases}


@router.get("/api/pib/releases/{prid}")
async def get_pib_release(prid: int):
    db = get_db()
    release = await db.pib_releases.find_one({"prid": prid}, {"_id": 0})
    if not release:
        raise HTTPException(status_code=404, detail="Release not found")

    analysis = await db.pib_analysis.find_one({"prid": prid}, {"_id": 0})
    return {"release": release, "analysis": analysis}


@router.get("/api/pib/search")
async def search_pib(
    theme: str = "",
    sub_theme: str = "",
    company: str = "",
    sentiment: str = "",
    ministry: str = "",
    impact: str = "",
    market_only: str = "true",  # Default: show only market-relevant
    sort: str = "impact",  # "impact" (high→low) or "date" (newest first)
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """Search analyzed PIB releases by theme, company, sentiment, ministry."""
    db = get_db()
    query_filter: dict = {}

    if theme:
        query_filter["themes.primary"] = {"$regex": theme, "$options": "i"}
    if sub_theme:
        query_filter["themes.sub_theme"] = {"$regex": sub_theme, "$options": "i"}
    if company:
        query_filter["affected_companies.symbol"] = company.upper()
    if sentiment:
        query_filter["sentiment"] = sentiment.lower()
    if ministry:
        query_filter["ministry"] = {"$regex": ministry, "$options": "i"}
    if impact:
        query_filter["impact_magnitude"] = impact.lower()

    # Default: exclude non-market releases (low impact + neutral + no companies)
    if market_only == "true" and not any([theme, company, impact]):
        query_filter["$or"] = [
            {"impact_magnitude": {"$in": ["high", "medium"]}},
            {"affected_companies.0": {"$exists": True}},
        ]

    skip = (page - 1) * per_page
    total = await db.pib_analysis.count_documents(query_filter)

    # Sort: impact (high→medium→low then by date) or date
    if sort == "impact":
        # Use aggregation for custom sort order
        pipeline = [
            {"$match": query_filter},
            {"$addFields": {
                "_impact_order": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$impact_magnitude", "high"]}, "then": 0},
                            {"case": {"$eq": ["$impact_magnitude", "medium"]}, "then": 1},
                        ],
                        "default": 2,
                    }
                }
            }},
            {"$sort": {"_impact_order": 1, "published_at": -1}},
            {"$skip": skip},
            {"$limit": per_page},
            {"$project": {"_id": 0, "_impact_order": 0}},
        ]
        results = await db.pib_analysis.aggregate(pipeline).to_list(length=per_page)
    else:
        cursor = (
            db.pib_analysis.find(query_filter, {"_id": 0})
            .sort("published_at", -1)
            .skip(skip)
            .limit(per_page)
        )
        results = await cursor.to_list(length=per_page)

    return {"total": total, "page": page, "per_page": per_page, "results": results}


@router.get("/api/pib/company/{symbol}")
async def get_pib_by_company(
    symbol: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """Get all PIB releases affecting a specific company."""
    db = get_db()
    symbol_upper = symbol.upper()

    skip = (page - 1) * per_page
    total = await db.pib_company_links.count_documents({"symbol": symbol_upper})
    cursor = (
        db.pib_company_links.find({"symbol": symbol_upper}, {"_id": 0})
        .sort("published_at", -1)
        .skip(skip)
        .limit(per_page)
    )
    links = await cursor.to_list(length=per_page)

    return {"symbol": symbol_upper, "total": total, "page": page, "per_page": per_page, "links": links}


@router.get("/api/pib/stats")
async def pib_stats():
    """PIB collection statistics: counts by ministry, theme, sentiment."""
    db = get_db()
    total_releases = await db.pib_releases.count_documents({})
    total_analyzed = await db.pib_analysis.count_documents({})
    pending = await db.pib_releases.count_documents({"analyzed": {"$ne": True}})

    # By ministry
    pipeline_ministry = [
        {"$group": {"_id": "$ministry", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 30},
    ]
    by_ministry = {
        doc["_id"]: doc["count"]
        async for doc in db.pib_releases.aggregate(pipeline_ministry)
        if doc["_id"]
    }

    # By theme
    pipeline_theme = [
        {"$unwind": "$themes"},
        {"$group": {"_id": "$themes.primary", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    by_theme = {
        doc["_id"]: doc["count"]
        async for doc in db.pib_analysis.aggregate(pipeline_theme)
        if doc["_id"]
    }

    # By sentiment
    pipeline_sentiment = [
        {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}},
    ]
    by_sentiment = {
        doc["_id"]: doc["count"]
        async for doc in db.pib_analysis.aggregate(pipeline_sentiment)
        if doc["_id"]
    }

    # Top affected companies
    pipeline_companies = [
        {"$group": {"_id": "$symbol", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20},
    ]
    top_companies = {
        doc["_id"]: doc["count"]
        async for doc in db.pib_company_links.aggregate(pipeline_companies)
        if doc["_id"]
    }

    return {
        "total_releases": total_releases,
        "total_analyzed": total_analyzed,
        "pending_analysis": pending,
        "by_ministry": by_ministry,
        "by_theme": by_theme,
        "by_sentiment": by_sentiment,
        "top_affected_companies": top_companies,
    }


@router.post("/api/pib/scrape")
async def trigger_pib_scrape():
    """Manually trigger PIB scraping."""
    from app.scrapers.pib import PIBScraper
    scraper = PIBScraper()
    count = await scraper.scrape_latest()
    return {"source": "pib", "saved": count, "timestamp": datetime.utcnow().isoformat()}


@router.post("/api/pib/analyze")
async def trigger_pib_analyze(limit: int = Query(20, ge=1, le=200)):
    """Manually trigger LLM analysis on pending PIB releases."""
    from app.services.pib_analyzer import analyze_pending
    results = await analyze_pending(limit=limit)
    return results


@router.post("/api/pib/analyze/{prid}")
async def trigger_pib_analyze_single(prid: int):
    """Analyze a single PIB release."""
    from app.services.pib_analyzer import analyze_release
    result = await analyze_release(prid)
    if not result:
        raise HTTPException(status_code=500, detail="Analysis failed")
    result.pop("_id", None)
    return result


_backfill_status = {"running": False, "progress": {}}


@router.post("/api/pib/backfill")
async def trigger_pib_backfill(
    days: int = Query(365, ge=1, le=730),
    background_tasks: "BackgroundTasks" = None,  # noqa: F821
):
    """Trigger historical backfill of PIB releases (runs in background)."""
    import asyncio
    from fastapi import BackgroundTasks as BT

    if _backfill_status["running"]:
        return {"status": "already_running", "progress": _backfill_status["progress"]}

    async def _run_backfill(days: int):
        from app.scrapers.pib import PIBScraper
        _backfill_status["running"] = True
        _backfill_status["progress"] = {"days": days, "saved": 0, "status": "starting"}
        try:
            scraper = PIBScraper()
            result = await scraper.backfill(days=days)
            _backfill_status["progress"] = {**result, "status": "completed"}
        except Exception as e:
            _backfill_status["progress"] = {"status": "error", "error": str(e)}
        finally:
            _backfill_status["running"] = False

    asyncio.create_task(_run_backfill(days))
    return {"status": "started", "days": days, "message": "Backfill running in background. Check GET /api/pib/backfill/status"}


@router.get("/api/pib/backfill/status")
async def backfill_status():
    """Check backfill progress."""
    return {"running": _backfill_status["running"], "progress": _backfill_status["progress"]}


# ── Parivesh Endpoints ────────────────────────────────────────────

@router.get("/api/parivesh/proposals")
async def list_parivesh_proposals(
    symbol: str = "",
    status: str = "",
    clearance_type: str = "",
    q: str = "",
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    db = get_db()
    query_filter: dict = {}

    if symbol:
        query_filter["$or"] = [
            {"nse_symbol": symbol.upper()},
            {"matched_symbols": symbol.upper()},
        ]
    if status:
        query_filter["proposal_status"] = {"$regex": status, "$options": "i"}
    if clearance_type:
        query_filter["clearance_type"] = {"$regex": clearance_type, "$options": "i"}
    if q:
        query_filter["$text"] = {"$search": q}

    skip = (page - 1) * per_page
    total = await db.parivesh_proposals.count_documents(query_filter)
    cursor = (
        db.parivesh_proposals.find(query_filter, {"_id": 0})
        .sort("scraped_at", -1)
        .skip(skip)
        .limit(per_page)
    )
    proposals = await cursor.to_list(length=per_page)

    return {"total": total, "page": page, "per_page": per_page, "proposals": proposals}


@router.get("/api/parivesh/proposals/{proposal_no:path}")
async def get_parivesh_proposal(proposal_no: str):
    db = get_db()
    proposal = await db.parivesh_proposals.find_one(
        {"proposal_no": proposal_no}, {"_id": 0}
    )
    if not proposal:
        raise HTTPException(status_code=404, detail="Proposal not found")

    docs = await db.parivesh_documents.find(
        {"proposal_no": proposal_no}, {"_id": 0}
    ).to_list(length=100)

    return {"proposal": proposal, "documents": docs}


@router.get("/api/parivesh/company/{symbol}")
async def get_parivesh_by_company(
    symbol: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """Get all Parivesh proposals for a company by NSE symbol."""
    db = get_db()
    symbol_upper = symbol.upper()
    query_filter = {
        "$or": [
            {"nse_symbol": symbol_upper},
            {"matched_symbols": symbol_upper},
        ]
    }

    skip = (page - 1) * per_page
    total = await db.parivesh_proposals.count_documents(query_filter)
    cursor = (
        db.parivesh_proposals.find(query_filter, {"_id": 0})
        .sort("scraped_at", -1)
        .skip(skip)
        .limit(per_page)
    )
    proposals = await cursor.to_list(length=per_page)

    return {"symbol": symbol_upper, "total": total, "page": page, "per_page": per_page, "proposals": proposals}


@router.get("/api/parivesh/stats")
async def parivesh_stats():
    """Parivesh collection statistics."""
    db = get_db()
    total_proposals = await db.parivesh_proposals.count_documents({})
    total_documents = await db.parivesh_documents.count_documents({})
    docs_pending = await db.parivesh_proposals.count_documents(
        {"documents_fetched": {"$ne": True}}
    )

    # Companies searched
    meta = await db.parivesh_meta.find_one({"_id": "parivesh_searched_companies"})
    companies_searched = len(meta.get("symbols", [])) if meta else 0

    # By clearance type
    pipeline_ct = [
        {"$group": {"_id": "$clearance_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    by_clearance = {
        doc["_id"]: doc["count"]
        async for doc in db.parivesh_proposals.aggregate(pipeline_ct)
        if doc["_id"]
    }

    # By status
    pipeline_status = [
        {"$group": {"_id": "$proposal_status", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20},
    ]
    by_status = {
        doc["_id"]: doc["count"]
        async for doc in db.parivesh_proposals.aggregate(pipeline_status)
        if doc["_id"]
    }

    # Top companies by proposals
    pipeline_companies = [
        {"$unwind": "$matched_symbols"},
        {"$group": {"_id": "$matched_symbols", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20},
    ]
    top_companies = {
        doc["_id"]: doc["count"]
        async for doc in db.parivesh_proposals.aggregate(pipeline_companies)
        if doc["_id"]
    }

    return {
        "total_proposals": total_proposals,
        "total_documents": total_documents,
        "documents_pending": docs_pending,
        "companies_searched": companies_searched,
        "by_clearance_type": by_clearance,
        "by_status": by_status,
        "top_companies": top_companies,
    }


_parivesh_status = {"running": False, "progress": {}}


@router.post("/api/parivesh/scrape")
async def trigger_parivesh_scrape(
    batch_size: int = Query(50, ge=1, le=500),
):
    """Search Parivesh for Nifty 500 companies (runs in background)."""
    import asyncio

    if _parivesh_status["running"]:
        return {"status": "already_running", "progress": _parivesh_status["progress"]}

    async def _run(bs: int):
        from app.scrapers.parivesh import PariveshScraper
        _parivesh_status["running"] = True
        _parivesh_status["progress"] = {"status": "starting", "batch_size": bs}
        try:
            scraper = PariveshScraper()
            result = await scraper.scrape_all(batch_size=bs)
            _parivesh_status["progress"] = {**result, "status": "completed"}
        except Exception as e:
            _parivesh_status["progress"] = {"status": "error", "error": str(e)}
        finally:
            _parivesh_status["running"] = False

    asyncio.create_task(_run(batch_size))
    return {"status": "started", "batch_size": batch_size, "message": "Scraping in background. Check GET /api/parivesh/scrape/status"}


@router.get("/api/parivesh/scrape/status")
async def parivesh_scrape_status():
    return {"running": _parivesh_status["running"], "progress": _parivesh_status["progress"]}


@router.post("/api/parivesh/documents")
async def trigger_parivesh_documents(
    limit: int = Query(50, ge=1, le=200),
):
    """Fetch documents for proposals that haven't had docs fetched yet."""
    from app.scrapers.parivesh import PariveshScraper
    scraper = PariveshScraper()
    result = await scraper.fetch_pending_documents(limit=limit)
    return result


@router.post("/api/parivesh/search")
async def parivesh_search_company(
    company: str = Query(..., description="Company name to search on Parivesh"),
):
    """Search Parivesh for a specific company name (ad-hoc, doesn't save)."""
    import httpx
    from app.scrapers.parivesh import BROWSER_HEADERS, SEARCH_URL, REQUEST_TIMEOUT

    async with httpx.AsyncClient(headers=BROWSER_HEADERS) as client:
        try:
            resp = await client.get(
                SEARCH_URL,
                params={"text": company},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            # API returns {"data": [...]} or a plain list
            if isinstance(data, dict) and "data" in data:
                items = data["data"] if isinstance(data["data"], list) else []
            elif isinstance(data, list):
                items = data
            else:
                items = []
            return {"query": company, "count": len(items), "results": items}
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Parivesh API error: {str(e)}")
