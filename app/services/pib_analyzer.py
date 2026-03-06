import json
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

import anthropic

from app.database import get_db

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_COMPANIES: list[dict] | None = None
_THEMES: dict | None = None
_MINISTRY_MAP: dict | None = None


def _load_companies() -> list[dict]:
    global _COMPANIES
    if _COMPANIES is None:
        with open(_DATA_DIR / "nifty500_companies.json", "r", encoding="utf-8") as f:
            _COMPANIES = json.load(f)
    return _COMPANIES


def _load_themes() -> dict:
    global _THEMES
    if _THEMES is None:
        with open(_DATA_DIR / "theme_taxonomy.json", "r", encoding="utf-8") as f:
            _THEMES = json.load(f)
    return _THEMES


def _load_ministry_map() -> dict:
    global _MINISTRY_MAP
    if _MINISTRY_MAP is None:
        with open(_DATA_DIR / "ministry_sector_map.json", "r", encoding="utf-8") as f:
            _MINISTRY_MAP = json.load(f)
    return _MINISTRY_MAP


def _build_company_symbols_text(ministry: str = "") -> str:
    """Build a compact list of company symbols + names for the prompt.

    If a ministry is provided and mapped, prioritize relevant sectors
    but still include top companies from other sectors for broad matching.
    """
    companies = _load_companies()
    ministry_map = _load_ministry_map()

    # Find relevant sectors for this ministry
    relevant_sectors = set()
    if ministry:
        for ministry_key, info in ministry_map.items():
            if ministry_key.lower() in ministry.lower() or ministry.lower() in ministry_key.lower():
                relevant_sectors.update(s.lower() for s in info.get("sectors", []))

    lines = []
    if relevant_sectors:
        # Include ALL companies from relevant sectors + top 100 by mcap from others
        relevant = []
        others = []
        for c in companies:
            nse = c.get("nse_code", "")
            if not nse:
                continue
            entry = f"{nse} | {c.get('company_name', '')} | {c.get('full_name', '')} | {c.get('sector', '')}"
            if c.get("sector", "").lower() in relevant_sectors:
                relevant.append(entry)
            else:
                others.append(entry)
        lines = relevant + others[:100]
    else:
        # No ministry match — include top 200 by mcap (already sorted)
        for c in companies[:200]:
            nse = c.get("nse_code", "")
            if nse:
                lines.append(f"{nse} | {c.get('company_name', '')} | {c.get('full_name', '')} | {c.get('sector', '')}")

    return "\n".join(lines)


def _build_theme_text() -> str:
    """Build a compact theme taxonomy for the prompt."""
    themes = _load_themes()
    lines = []
    for primary, subs in themes.items():
        lines.append(f"{primary}: {', '.join(subs)}")
    return "\n".join(lines)


_ANALYSIS_PROMPT = """You are an Indian financial market analyst specializing in government policy impact on listed companies.

Analyze this PIB (Press Information Bureau) government press release and return a JSON object with these fields:

1. "summary": 2-3 sentence summary of the press release focused on market/business impact
2. "themes": array of objects like {{"primary": "Energy", "sub_theme": "Nuclear Energy"}}. Use ONLY from the taxonomy below.
3. "affected_companies": array of objects like {{"symbol": "ONGC", "relevance": "direct", "reason": "brief explanation"}}.
   - "relevance" must be "direct" (company explicitly mentioned or directly impacted) or "indirect" (sector/policy impact)
   - Match ONLY from the company list below. Be precise — don't guess.
4. "sentiment": "positive", "negative", or "neutral" — the likely market impact
5. "impact_magnitude": "high" (policy change, major allocation, regulatory shift), "medium" (incremental update, progress report), or "low" (routine, informational)
6. "key_policy_changes": array of specific policy changes, allocations, or decisions mentioned

IMPORTANT:
- Return ONLY valid JSON. No markdown, no code blocks, no explanation.
- If the release is purely ceremonial/greeting with no market relevance, return {{"summary":"Non-market-relevant release","themes":[],"affected_companies":[],"sentiment":"neutral","impact_magnitude":"low","key_policy_changes":[]}}
- Be conservative with company matching — only include companies with clear relevance.
- For ministry-level impacts, include the top 3-5 most affected companies in that sector.

THEME TAXONOMY:
{themes}

COMPANY UNIVERSE (Symbol | Short Name | Full Name | Sector):
{companies}

PRESS RELEASE:
Ministry: {ministry}
Title: {title}
Date: {date}
Content:
{content}"""


def _run_claude_sdk(prompt: str) -> str | None:
    """Call Claude via the Anthropic Python SDK (preferred for server use)."""
    try:
        client = anthropic.Anthropic()  # Uses ANTHROPIC_API_KEY env var
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()
    except anthropic.APIError as e:
        logger.error("Anthropic API error: %s", e)
        return None
    except Exception as e:
        logger.error("Claude SDK error: %s", e)
        return None


_PIB_JSON_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "themes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "primary": {"type": "string"},
                    "sub_theme": {"type": "string"},
                },
                "required": ["primary", "sub_theme"],
            },
        },
        "affected_companies": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string"},
                    "relevance": {"type": "string", "enum": ["direct", "indirect"]},
                    "reason": {"type": "string"},
                },
                "required": ["symbol", "relevance", "reason"],
            },
        },
        "sentiment": {"type": "string", "enum": ["positive", "negative", "neutral"]},
        "impact_magnitude": {"type": "string", "enum": ["high", "medium", "low"]},
        "key_policy_changes": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["summary", "themes", "affected_companies", "sentiment", "impact_magnitude", "key_policy_changes"],
})


def _run_claude_cli(prompt: str, model: str = "sonnet", timeout: int = 180) -> str | None:
    """Run Claude CLI as subprocess, matching brain repo concall pattern.

    Uses --output-format json and --json-schema for structured output.
    On Windows, long prompts (>30k) are piped via temp file.
    """
    claude_path = shutil.which("claude")
    if not claude_path:
        logger.error("Claude CLI not found in PATH")
        return None

    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    model_flag = f"claude-{model}-4-20250514"

    tmp_path = None
    try:
        if len(prompt) > 30000:
            # Write to temp file and pipe via shell for long prompts
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", encoding="utf-8", delete=False,
            )
            tmp.write(prompt)
            tmp.close()
            tmp_path = tmp.name

            cmd = (
                f'type "{tmp_path}" | "{claude_path}" -p'
                f' --model {model_flag}'
                f' --output-format json'
            )
            result = subprocess.run(
                cmd, shell=True,
                capture_output=True, text=True, timeout=timeout, env=env,
            )
        else:
            # Direct arg (matches brain repo pattern)
            result = subprocess.run(
                [claude_path, "-p", prompt,
                 "--model", model_flag,
                 "--output-format", "json",
                 "--json-schema", _PIB_JSON_SCHEMA],
                capture_output=True, text=True, timeout=timeout, env=env,
            )

        if result.returncode != 0:
            logger.error("Claude CLI failed (rc=%d): %s", result.returncode, result.stderr[:500])
            return None

        output = result.stdout.strip()
        if not output:
            logger.error("Claude CLI returned empty output")
            return None

        return output

    except subprocess.TimeoutExpired:
        logger.error("Claude CLI timed out after %ds", timeout)
        return None
    except Exception as e:
        logger.error("Claude CLI error: %s", e)
        return None
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def _run_claude(prompt: str) -> str | None:
    """Run Claude analysis — tries CLI first, falls back to SDK if API key set."""
    result = _run_claude_cli(prompt)
    if result:
        return result
    if os.environ.get("ANTHROPIC_API_KEY"):
        logger.info("CLI failed, falling back to Anthropic SDK")
        return _run_claude_sdk(prompt)
    return None


def _parse_analysis_json(raw: str) -> dict | None:
    """Parse Claude's response into structured analysis.

    Handles multiple formats:
    - --output-format json wraps in {"result": "...", "model": "...", ...}
    - Direct JSON object
    - JSON inside markdown code blocks
    """
    # First try: --output-format json envelope
    try:
        envelope = json.loads(raw)
        if isinstance(envelope, dict):
            if "result" in envelope:
                # The result field contains the actual analysis (may be string or dict)
                inner = envelope["result"]
                if isinstance(inner, dict):
                    return inner
                if isinstance(inner, str):
                    return _parse_analysis_json(inner)  # Recurse on inner string
            # Maybe it's directly the analysis object
            if "summary" in envelope:
                return envelope
    except json.JSONDecodeError:
        pass

    # Try extracting JSON from markdown code block
    import re
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Try finding first { to last }
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(raw[start:end + 1])
        except json.JSONDecodeError:
            pass

    logger.error("Failed to parse Claude response as JSON: %s", raw[:200])
    return None


async def analyze_release(prid: int) -> dict | None:
    """Analyze a single PIB release using Claude CLI."""
    db = get_db()

    # Check if already analyzed
    existing = await db.pib_analysis.find_one({"prid": prid})
    if existing:
        return existing

    # Load the release
    release = await db.pib_releases.find_one({"prid": prid})
    if not release:
        logger.error("Release not found: %d", prid)
        return None

    # Build prompt — pass ministry for sector-aware company filtering
    ministry = release.get("ministry", "Unknown")
    companies_text = _build_company_symbols_text(ministry)
    themes_text = _build_theme_text()

    # Truncate content to ~15k chars to stay within token limits
    content = release.get("full_text", "")[:15000]

    prompt = _ANALYSIS_PROMPT.format(
        themes=themes_text,
        companies=companies_text,
        ministry=ministry,
        title=release.get("title", ""),
        date=str(release.get("published_at", "")),
        content=content,
    )

    # Run Claude (SDK if API key available, else CLI)
    raw_response = _run_claude(prompt)
    if not raw_response:
        return None

    analysis = _parse_analysis_json(raw_response)
    if not analysis:
        return None

    # Enrich with metadata
    analysis["prid"] = prid
    analysis["ministry"] = release.get("ministry", "")
    analysis["title"] = release.get("title", "")
    analysis["published_at"] = release.get("published_at")
    analysis["analyzed_at"] = datetime.utcnow()

    # Save analysis
    try:
        await db.pib_analysis.insert_one(analysis)
    except Exception:
        # Update if exists
        await db.pib_analysis.replace_one({"prid": prid}, analysis, upsert=True)

    # Save company links (denormalized for fast queries)
    for company in analysis.get("affected_companies", []):
        link = {
            "prid": prid,
            "symbol": company.get("symbol", ""),
            "relevance": company.get("relevance", "indirect"),
            "reason": company.get("reason", ""),
            "ministry": release.get("ministry", ""),
            "published_at": release.get("published_at"),
            "title": release.get("title", ""),
            "sentiment": analysis.get("sentiment", "neutral"),
        }
        try:
            await db.pib_company_links.update_one(
                {"prid": prid, "symbol": company.get("symbol", "")},
                {"$set": link},
                upsert=True,
            )
        except Exception:
            logger.exception("Failed to save company link for PRID %d", prid)

    # Mark release as analyzed
    await db.pib_releases.update_one({"prid": prid}, {"$set": {"analyzed": True}})

    return analysis


async def analyze_pending(limit: int = 50) -> dict:
    """Analyze all unanalyzed PIB releases, up to limit."""
    db = get_db()
    cursor = db.pib_releases.find(
        {"analyzed": {"$ne": True}},
        {"prid": 1},
    ).sort("published_at", -1).limit(limit)

    results = {"analyzed": 0, "failed": 0, "skipped": 0, "errors": []}

    async for doc in cursor:
        prid = doc["prid"]
        try:
            analysis = await analyze_release(prid)
            if analysis:
                results["analyzed"] += 1
            else:
                results["failed"] += 1
                results["errors"].append({"prid": prid, "error": "Analysis returned None"})
        except Exception as e:
            results["failed"] += 1
            results["errors"].append({"prid": prid, "error": str(e)})
            logger.exception("Failed to analyze PRID %d", prid)

    return results
