"""Qdrant point ids must be integers or UUIDs; derive a stable UUID from the MIND article id."""
import uuid

_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "newsrec")


def point_id(article_id: str) -> str:
    return str(uuid.uuid5(_NAMESPACE, article_id))
