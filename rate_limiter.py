from fastapi import HTTPException, Request
import time
from collections import defaultdict

class RateLimiter:
    def __init__(self):
        self.requests = defaultdict(list)
    
    def is_rate_limited(self, key: str, limit: int, window: int):
        now = time.time()
        self.requests[key] = [req_time for req_time in self.requests[key] if now - req_time < window]
        
        if len(self.requests[key]) >= limit:
            return True
        
        self.requests[key].append(now)
        return False

rate_limiter = RateLimiter()

async def rate_limit_middleware(request: Request, call_next):
    # Skip rate limiting for health checks
    if request.url.path in ["/healthz", "/metrics", "/docs", "/openapi.json"]:
        return await call_next(request)
    
    client_ip = request.client.host if request.client else "unknown"
    rate_limit_key = f"{client_ip}:{request.url.path}"
    
    # 60 requests per minute per IP
    if rate_limiter.is_rate_limited(rate_limit_key, limit=60, window=60):
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Maximum 60 requests per minute.")
    
    response = await call_next(request)
    
    # Add rate limit headers
    response.headers["X-RateLimit-Limit"] = "60"
    response.headers["X-RateLimit-Remaining"] = str(60 - len(rate_limiter.requests[rate_limit_key]))
    
    return response