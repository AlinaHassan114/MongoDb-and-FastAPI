# GoodBooks API - Complete Assignment Solution

A production-style REST API for book ratings and recommendations built with FastAPI and MongoDB.

##  Features Implemented

### Core Requirements 
- **REST API** with FastAPI and proper HTTP status codes
- **MongoDB** data storage with optimized indexes
- **CSV Data Ingestion** with idempotent scripts
- **Authentication** with API key protection
- **Pagination, Filtering, Sorting** on all list endpoints
- **Input Validation** with Pydantic models
- **OpenAPI Documentation** automatically generated
- **Request Logging** with structured JSON logs
- **Error Handling** with consistent error responses
- **Docker Containerization** with docker-compose

### API Endpoints 
- `GET /books` - List books with search, filters, pagination
- `GET /books/{id}` - Get book details
- `GET /books/{id}/tags` - Get book tags
- `GET /books/{id}/ratings/summary` - Get rating statistics
- `GET /authors/{name}/books` - Get books by author
- `GET /tags` - List all tags with book counts
- `GET /users/{id}/to-read` - Get user's reading list
- `POST /ratings` - Create/update rating (protected)
- `GET /healthz` - Health check with DB connectivity
- `GET /metrics` - System metrics

### Nice-to-Have Features 
- **Rate Limiting** - 60 requests/minute per IP
- **Recommendations** - Simple book recommendations
- **Comprehensive Testing** - pytest test suite
- **CORS Support** - Cross-origin requests enabled
- **Structured Logging** - JSON logs for production

## Project Structure