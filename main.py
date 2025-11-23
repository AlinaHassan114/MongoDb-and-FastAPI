from fastapi import FastAPI, Query, HTTPException, Depends, Request, status, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import time
import logging
import json
import os
from datetime import datetime
from pymongo import MongoClient

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="GoodBooks API",
    description="A REST API for book ratings and recommendations",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "goodbooks")
API_KEY = os.getenv("API_KEY", "dev-key-123")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Auth dependency
async def require_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key

# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = int((time.time() - start_time) * 1000)
    
    log_data = {
        "route": request.url.path,
        "method": request.method,
        "query_params": dict(request.query_params),
        "status_code": response.status_code,
        "latency_ms": process_time,
        "client_ip": request.client.host if request.client else None,
        "timestamp": datetime.now().isoformat()
    }
    
    logger.info(json.dumps(log_data))
    return response

# Pydantic models
class RatingIn(BaseModel):
    user_id: int
    book_id: int
    rating: int = Field(ge=1, le=5)

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

# Health check with MongoDB ping
@app.get("/healthz")
async def health_check():
    try:
        db.command('ping')
        return {
            "status": "healthy", 
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection failed: {str(e)}"
        )

# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "collections": {
            "books": db.books.count_documents({}),
            "ratings": db.ratings.count_documents({}),
            "tags": db.tags.count_documents({}),
            "book_tags": db.book_tags.count_documents({}),
            "to_read": db.to_read.count_documents({})
        }
    }
    return metrics

# Books endpoints
@app.get("/books", response_model=PaginatedResponse)
async def list_books(
    q: Optional[str] = None,
    tag: Optional[str] = None,
    min_avg: Optional[float] = Query(None, ge=1, le=5, description="Minimum average rating"),
    year_from: Optional[int] = Query(None, ge=1000, le=2100, description="Start year"),
    year_to: Optional[int] = Query(None, ge=1000, le=2100, description="End year"),
    sort: str = Query("avg", regex="^(avg|ratings_count|year|title)$"),
    order: str = Query("desc", regex="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    # Build filter
    filter_query = {}
    
    # Text search
    if q:
        filter_query["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"authors": {"$regex": q, "$options": "i"}}
        ]
    
    # Rating filter
    if min_avg is not None:
        filter_query["average_rating"] = {"$gte": min_avg}
    
    # Year range filter
    year_filter = {}
    if year_from is not None:
        year_filter["$gte"] = year_from
    if year_to is not None:
        year_filter["$lte"] = year_to
    if year_filter:
        filter_query["original_publication_year"] = year_filter
    
    # Sort mapping
    sort_map = {
        "avg": "average_rating",
        "ratings_count": "ratings_count", 
        "year": "original_publication_year",
        "title": "title"
    }
    
    sort_field = sort_map.get(sort, "average_rating")
    sort_direction = -1 if order == "desc" else 1
    
    # Get total count
    total = db.books.count_documents(filter_query)
    
    # Get paginated results
    skip = (page - 1) * page_size
    books_cursor = db.books.find(filter_query).sort(sort_field, sort_direction).skip(skip).limit(page_size)
    
    books = []
    for book in books_cursor:
        book["_id"] = str(book["_id"])
        books.append(book)
    
    return {
        "items": books,
        "page": page,
        "page_size": page_size,
        "total": total
    }

@app.get("/books/{book_id}")
async def get_book(book_id: int):
    book = db.books.find_one({"book_id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    book["_id"] = str(book["_id"])
    return book

@app.get("/books/{book_id}/tags")
async def get_book_tags(book_id: int):
    book = db.books.find_one({"book_id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    goodreads_id = book["goodreads_book_id"]
    
    pipeline = [
        {"$match": {"goodreads_book_id": goodreads_id}},
        {"$lookup": {
            "from": "tags",
            "localField": "tag_id", 
            "foreignField": "tag_id",
            "as": "tag_info"
        }},
        {"$unwind": "$tag_info"},
        {"$project": {
            "tag_id": 1,
            "tag_name": "$tag_info.tag_name",
            "count": 1
        }},
        {"$sort": {"count": -1}}
    ]
    
    tags = list(db.book_tags.aggregate(pipeline))
    
    for tag in tags:
        tag["_id"] = str(tag["_id"])
    
    return tags

@app.get("/authors/{author_name}/books")
async def get_author_books(author_name: str, page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100)):
    filter_query = {"authors": {"$regex": author_name, "$options": "i"}}
    
    total = db.books.count_documents(filter_query)
    skip = (page - 1) * page_size
    
    books_cursor = db.books.find(filter_query).sort("average_rating", -1).skip(skip).limit(page_size)
    
    books = []
    for book in books_cursor:
        book["_id"] = str(book["_id"])
        books.append(book)
    
    return {
        "items": books,
        "page": page,
        "page_size": page_size,
        "total": total
    }

# Tags endpoints
@app.get("/tags", response_model=PaginatedResponse)
async def list_tags(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    pipeline = [
        {"$lookup": {
            "from": "book_tags",
            "localField": "tag_id",
            "foreignField": "tag_id", 
            "as": "book_links"
        }},
        {"$addFields": {
            "book_count": {"$size": "$book_links"}
        }},
        {"$sort": {"book_count": -1}},
        {"$skip": (page - 1) * page_size},
        {"$limit": page_size},
        {"$project": {
            "tag_id": 1,
            "tag_name": 1,
            "book_count": 1
        }}
    ]
    
    tags_with_counts = list(db.tags.aggregate(pipeline))
    
    total = db.tags.count_documents({})
    
    for tag in tags_with_counts:
        tag["_id"] = str(tag["_id"])
    
    return {
        "items": tags_with_counts,
        "page": page,
        "page_size": page_size,
        "total": total
    }

# User endpoints
@app.get("/users/{user_id}/to-read")
async def get_user_to_read(user_id: int):
    to_read_books = list(db.to_read.find({"user_id": user_id}))
    
    book_ids = [item["book_id"] for item in to_read_books]
    books = list(db.books.find({"book_id": {"$in": book_ids}}))
    
    for book in books:
        book["_id"] = str(book["_id"])
    
    return books

@app.get("/books/{book_id}/ratings/summary")
async def get_ratings_summary(book_id: int):
    book = db.books.find_one({"book_id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    pipeline = [
        {"$match": {"book_id": book_id}},
        {"$group": {
            "_id": "$book_id",
            "average_rating": {"$avg": "$rating"},
            "ratings_count": {"$sum": 1},
            "histogram": {
                "$push": "$rating"
            }
        }}
    ]
    
    result = list(db.ratings.aggregate(pipeline))
    
    if not result:
        return RatingSummary(
            book_id=book_id,
            average_rating=0,
            ratings_count=0,
            histogram={1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        )
    
    stats = result[0]
    
    histogram = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    for rating in stats["histogram"]:
        histogram[rating] += 1
    
    return RatingSummary(
        book_id=book_id,
        average_rating=round(stats["average_rating"], 2),
        ratings_count=stats["ratings_count"],
        histogram=histogram
    )

# Protected endpoints
@app.post("/ratings", response_model=dict)
async def upsert_rating(rating: RatingIn, api_key: str = Depends(require_api_key)):
    book = db.books.find_one({"book_id": rating.book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    result = db.ratings.update_one(
        {"user_id": rating.user_id, "book_id": rating.book_id},
        {"$set": rating.dict()},
        upsert=True
    )
    
    if result.upserted_id:
        return {"status": "created", "message": "Rating created successfully"}
    else:
        return {"status": "updated", "message": "Rating updated successfully"}

# Recommendations endpoint 
@app.get("/users/{user_id}/recommendations")
async def get_recommendations(user_id: int, top_k: int = Query(20, ge=1, le=100)):
    """
    Simple recommendation based on user's highly rated books and their tags
    """
    # Get user's highly rated books
    user_ratings = list(db.ratings.find({"user_id": user_id, "rating": {"$gte": 4}}))
    
    if not user_ratings:
        # If no ratings, return popular books
        popular_books = list(db.books.find().sort("ratings_count", -1).limit(top_k))
        books = []
        for book in popular_books:
            book["_id"] = str(book["_id"])
            books.append(book)
        return {"recommendations": books, "type": "popular"}
    
    # Get book IDs from user's ratings
    rated_book_ids = [r["book_id"] for r in user_ratings]
    
    # Get books with similar tags (simplified approach)
    recommended_books = list(db.books.find({
        "book_id": {"$nin": rated_book_ids}
    }).sort("average_rating", -1).limit(top_k))
    
    books = []
    for book in recommended_books:
        book["_id"] = str(book["_id"])
        books.append(book)
    
    return {"recommendations": books, "type": "based_on_ratings"}

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

@app.get("/")
def read_root():
    return {"message": "GoodBooks API is running"}