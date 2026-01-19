from typing import List, Optional
from pydantic import BaseModel
z  # api/schemas.py


class MessageResponse(BaseModel):
    message_id: int
    message_text: str
    view_count: int


class TopProductResponse(BaseModel):
    product: str
    mention_count: int


class ActivityResponse(BaseModel):
    date_key: str
    post_count: int
    total_views: int
