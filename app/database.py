import logging

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import settings

logger = logging.getLogger(__name__)

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None


async def connect_db() -> None:
    global _client, _db
    _client = AsyncIOMotorClient(settings.mongo_uri)
    _db = _client[settings.mongo_db]

    # Create unique index on url_hash for dedup
    await _db.articles.create_index("url_hash", unique=True)
    await _db.articles.create_index("scraped_at")
    await _db.articles.create_index("source_name")
    await _db.articles.create_index("tags")

    # PIB collections
    await _db.pib_releases.create_index("prid", unique=True)
    await _db.pib_releases.create_index("published_at")
    await _db.pib_releases.create_index("ministry")
    await _db.pib_releases.create_index("analyzed")
    await _db.pib_releases.create_index(
        [("title", "text"), ("full_text", "text")],
        name="pib_text_search",
        default_language="english",
    )

    await _db.pib_analysis.create_index("prid", unique=True)
    await _db.pib_analysis.create_index("themes.primary")
    await _db.pib_analysis.create_index("themes.sub_theme")
    await _db.pib_analysis.create_index("affected_companies.symbol")
    await _db.pib_analysis.create_index("sentiment")
    await _db.pib_analysis.create_index("published_at")

    await _db.pib_company_links.create_index([("prid", 1), ("symbol", 1)], unique=True)
    await _db.pib_company_links.create_index("symbol")
    await _db.pib_company_links.create_index("published_at")

    # Parivesh collections
    await _db.parivesh_proposals.create_index("proposal_no", unique=True)
    await _db.parivesh_proposals.create_index("nse_symbol")
    await _db.parivesh_proposals.create_index("matched_symbols")
    await _db.parivesh_proposals.create_index("proposal_status")
    await _db.parivesh_proposals.create_index("clearance_type")
    await _db.parivesh_proposals.create_index("documents_fetched")
    await _db.parivesh_proposals.create_index("analyzed")
    await _db.parivesh_proposals.create_index(
        [("project_name", "text"), ("company_name", "text")],
        name="parivesh_text_search",
        default_language="english",
    )

    await _db.parivesh_documents.create_index(
        [("proposal_no", 1), ("doc_uuid", 1)], unique=True
    )
    await _db.parivesh_documents.create_index("proposal_no")
    await _db.parivesh_documents.create_index("doc_type")

    logger.info("Connected to MongoDB: %s", settings.mongo_db)


async def close_db() -> None:
    global _client, _db
    if _client:
        _client.close()
        _client = None
        _db = None
    logger.info("Disconnected from MongoDB")


def get_db() -> AsyncIOMotorDatabase:
    if _db is None:
        raise RuntimeError("Database not initialized. Call connect_db() first.")
    return _db
