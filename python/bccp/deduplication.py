"""URL deduplication service for multi-crawl processing."""

import hashlib
import json
import time
from typing import Optional, Union, List, cast
from urllib.parse import urlparse

import redis

from .logging import log_with_fields, setup_logger


class URLDeduplicator:
    """Service for tracking processed URLs across multiple crawls using Redis."""
    
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        key_prefix: str = "bccp:urls:",
    ):
        """Initialize URL deduplicator with Redis backend.
        
        Args:
            redis_host: Redis server host
            redis_port: Redis server port  
            redis_db: Redis database number
            key_prefix: Prefix for Redis keys
            
        Raises:
            redis.ConnectionError: If Redis connection fails
        """
        self.logger = setup_logger("deduplicator")
        self.key_prefix = key_prefix
        
        # Connect to Redis (required for this feature)
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        
        # Test connection
        try:
            self.redis_client.ping()
            log_with_fields(
                self.logger,
                "info",
                "Connected to Redis for URL deduplication",
                host=redis_host,
                port=redis_port,
                db=redis_db,
            )
        except redis.ConnectionError:
            log_with_fields(
                self.logger,
                "error",
                "Failed to connect to Redis - deduplication requires Redis",
                host=redis_host,
                port=redis_port,
            )
            raise
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL for consistent deduplication.
        
        Args:
            url: Raw URL string
            
        Returns:
            Normalized URL string
        """
        try:
            parsed = urlparse(url.lower().strip())
            # Remove fragment and sort query parameters for consistency
            normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if parsed.query:
                # Sort query parameters for consistent representation
                query_parts = sorted(parsed.query.split('&'))
                normalized += f"?{'&'.join(query_parts)}"
            return normalized
        except Exception:
            # If URL parsing fails, use the original URL
            return url.lower().strip()
    
    def _url_hash(self, url: str) -> str:
        """Generate a hash for the URL to use as a key.
        
        Args:
            url: Normalized URL
            
        Returns:
            SHA256 hash of the URL (first 16 characters)
        """
        return hashlib.sha256(url.encode('utf-8')).hexdigest()[:16]
    
    def try_reserve_url(self, url: str, crawl_version: str) -> bool:
        """Atomically try to reserve a URL for processing.
        
        Args:
            url: URL to reserve
            crawl_version: Crawl version where this URL was found
            
        Returns:
            True if URL was successfully reserved, False if already reserved/processed
        """
        normalized_url = self._normalize_url(url)
        url_key = self._url_hash(normalized_url)
        key = f"{self.key_prefix}{url_key}"
        
        try:
            # Use SET with NX (not exists) for atomic reservation
            reservation_data = json.dumps({
                "url": normalized_url,
                "crawl_version": crawl_version,
                "status": "reserved",
                "reserved_at": int(time.time()),
            })
            
            # Returns True if key was set (didn't exist), False if key already exists
            result = self.redis_client.set(key, reservation_data, nx=True)
            
            if result:
                log_with_fields(
                    self.logger,
                    "debug",
                    "Reserved URL for processing",
                    url=url,
                    crawl_version=crawl_version,
                )
            
            return result is not None
            
        except Exception as e:
            log_with_fields(
                self.logger,
                "error",
                "Failed to reserve URL",
                url=url,
                crawl_version=crawl_version,
                error=str(e),
            )
            # On error, assume URL not reserved to avoid losing data
            return False
    
    def is_url_processed(self, url: str) -> bool:
        """Check if a URL has already been processed or reserved.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL was already processed/reserved, False otherwise
        """
        normalized_url = self._normalize_url(url)
        url_key = self._url_hash(normalized_url)
        key = f"{self.key_prefix}{url_key}"
        
        try:
            return self.redis_client.exists(key) == 1
        except Exception as e:
            log_with_fields(
                self.logger,
                "error",
                "Failed to check URL deduplication",
                url=url,
                error=str(e),
            )
            # On error, assume URL not processed to avoid losing data
            return False
    
    def mark_url_processed(self, url: str, success: bool, error_reason: str = "") -> bool:
        """Mark a reserved URL as processed (completed or failed).
        
        Args:
            url: URL to mark as processed
            success: True if processing succeeded, False if failed
            error_reason: Reason for failure (only used if success=False)
            
        Returns:
            True if successfully marked, False otherwise
        """
        normalized_url = self._normalize_url(url)
        url_key = self._url_hash(normalized_url)
        key = f"{self.key_prefix}{url_key}"
        
        try:
            # Get existing data and update status
            existing_data: Optional[str] = cast(Optional[str], self.redis_client.get(key))
            if existing_data:
                data = json.loads(existing_data)
                data["status"] = "completed" if success else "failed"
                data["processed_at"] = int(time.time())
                
                if not success and error_reason:
                    data["error_reason"] = error_reason
                
                self.redis_client.set(key, json.dumps(data))
                
                log_with_fields(
                    self.logger,
                    "debug",
                    f"Marked URL as {'completed' if success else 'failed'}",
                    url=url,
                    success=success,
                    error_reason=error_reason if not success else None,
                    crawl_version=data.get("crawl_version", "unknown"),
                )
                return True
            else:
                log_with_fields(
                    self.logger,
                    "warning",
                    "Attempted to mark non-reserved URL as processed",
                    url=url,
                    success=success,
                )
                return False
                
        except Exception as e:
            log_with_fields(
                self.logger,
                "error",
                "Failed to mark URL as processed",
                url=url,
                success=success,
                error=str(e),
            )
            return False
    
    def get_processed_count(self) -> int:
        """Get the total number of processed URLs.
        
        Returns:
            Number of processed URLs, or -1 if unavailable
        """
        try:
            # Count keys with our prefix
            pattern = f"{self.key_prefix}*"
            keys: List[str] = cast(List[str], self.redis_client.keys(pattern))
            return len(keys)
        except Exception as e:
            log_with_fields(
                self.logger,
                "error",
                "Failed to get processed URL count",
                error=str(e),
            )
            return -1
    
    def get_url_info(self, url: str) -> Optional[dict]:
        """Get information about a processed URL.
        
        Args:
            url: URL to look up
            
        Returns:
            Dictionary with URL metadata if found, None otherwise
        """
        normalized_url = self._normalize_url(url)
        url_key = self._url_hash(normalized_url)
        key = f"{self.key_prefix}{url_key}"
        
        try:
            data: Optional[str] = cast(Optional[str], self.redis_client.get(key))
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            log_with_fields(
                self.logger,
                "error",
                "Failed to get URL info",
                url=url,
                error=str(e),
            )
            return None
    
    def clear_all(self) -> bool:
        """Clear all processed URLs (mainly for testing).
        
        Returns:
            True if successfully cleared, False otherwise
        """
        try:
            pattern = f"{self.key_prefix}*"
            keys: List[str] = cast(List[str], self.redis_client.keys(pattern))
            if keys:
                self.redis_client.delete(*keys)
            log_with_fields(
                self.logger,
                "info",
                "Cleared all processed URLs",
                count=len(keys) if keys else 0,
            )
            return True
        except Exception as e:
            log_with_fields(
                self.logger,
                "error",
                "Failed to clear processed URLs",
                error=str(e),
            )
            return False