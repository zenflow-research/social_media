from app.config import settings
from app.models.article import Article


def tag_article(article: Article) -> None:
    """Add keyword-based tags to an article by matching against title + summary."""
    text = f"{article.title} {article.summary}".lower()
    matched = [kw for kw in settings.keyword_list if kw in text]
    # Merge with any existing tags, deduplicate
    article.tags = list(set(article.tags + matched))
