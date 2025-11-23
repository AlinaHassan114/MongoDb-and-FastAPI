from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

class Book(BaseModel):
    book_id: int
    goodreads_book_id: int
    title: str
    authors: str
    original_publication_year: Optional[int] = None
    average_rating: Optional[float] = None
    ratings_count: Optional[int] = None
    image_url: Optional[str] = None
    small_image_url: Optional[str] = None

class BookResponse(Book):
    class Config:
        from_attributes = True

class RatingIn(BaseModel):
    user_id: int
    book_id: int
    rating: int = Field(ge=1, le=5)

class RatingResponse(BaseModel):
    user_id: int
    book_id: int
    rating: int
    created_at: Optional[datetime] = None

class Tag(BaseModel):
    tag_id: int
    tag_name: str

class BookTag(BaseModel):
    goodreads_book_id: int
    tag_id: int
    count: int

class ToRead(BaseModel):
    user_id: int
    book_id: int

class PaginatedResponse(BaseModel):
    items: List[Any]
    page: int
    page_size: int
    total: int

class RatingSummary(BaseModel):
    book_id: int
    average_rating: float
    ratings_count: int
    histogram: Dict[int, int]