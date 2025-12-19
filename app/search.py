from fastapi import APIRouter, Query
from app.embedding_utils import embed_text
from pydantic import BaseModel
from typing import List
from app.models import Product, Service
import json
import logging

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SearchResultItem(BaseModel):
    id: str
    similarity: float

class SearchResponse(BaseModel):
    products: List[SearchResultItem] = []
    services: List[SearchResultItem] = []


router = APIRouter()

@router.get("/", response_model=SearchResponse)
async def search(query: str = Query(..., description="Search query"),
                 limit: int = Query(20, description="Number of results per type")):
    """
    Search both products and services using semantic embeddings + cosine similarity
    Returns top `limit` products and top `limit` services
    """

    # 1️⃣ Generate a single embedding for the query
    query_embedding = await embed_text(query)
    
    # Import pool inside the function to ensure it's initialized
    from app.db import pool

    # Initialize empty lists for results
    products = []
    services = []

    try:
        async with pool.acquire() as conn:

            # 2️⃣ Search products (single cosine similarity using pgvector)
            products_rows = await conn.fetch("""
                SELECT p.*, 1 - (pe.embedding <=> $1) AS score
                FROM product_embeddings pe
                JOIN products p ON pe.product_id = p.id
                ORDER BY 1 - (embedding <=> $1) DESC
                LIMIT $2
            """, query_embedding, limit)

            # 3️⃣ Search services (single cosine similarity using pgvector)
            services_rows = await conn.fetch("""
                SELECT s.*, 1 - (se.embedding <=> $1) AS score
                FROM service_embeddings se
                JOIN services s ON se.service_id = s.id
                ORDER BY 1 - (embedding <=> $1) DESC
                LIMIT $2
            """, query_embedding, limit)

        # 4️⃣ Convert DB rows to simplified search result items
        products = []
        for row in products_rows:
            row_dict = dict(row)
            # Get the score and ID
            score = row_dict.get('score', 0)
            product_id = row_dict.get('id', 'unknown')
            logger.info(f"Product '{row_dict.get('name', 'Unknown')}' score: {score}")
            products.append(SearchResultItem(id=product_id, similarity=score))
        
        services = []
        for row in services_rows:
            row_dict = dict(row)
            # Get the score and ID
            score = row_dict.get('score', 0)
            service_id = row_dict.get('id', 'unknown')
            logger.info(f"Service '{row_dict.get('name', 'Unknown')}' score: {score}")
            services.append(SearchResultItem(id=service_id, similarity=score))

    except Exception as e:
        # If there's any database error (e.g., tables don't exist yet),
        # return empty arrays instead of throwing an error
        logger.warning(f"Database error during search: {e}")
        # Continue with empty arrays

    # 5️⃣ Log results and return typed response
    # Will return empty arrays if no items found or if there was a database error
    logger.info(f"Search completed. Products found: {len(products)}, Services found: {len(services)}")
    if products:
        logger.info(f"Top product IDs: {[p.id for p in products[:5]]}")
    if services:
        logger.info(f"Top service IDs: {[s.id for s in services[:5]]}")
    
    return SearchResponse(products=products, services=services)