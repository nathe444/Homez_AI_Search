from fastapi import APIRouter, HTTPException
from app.embedding_utils import embed_text
from app.models import Service
import json

router = APIRouter()

@router.post("/")
async def ingest_service(service: Service):
    """
    Ingest a service at `/service` endpoint:
    - Store the service JSON as-is
    - Generate a single unified embedding from all service data
    - Store embedding using pgvector
    """
    
    # Import pool inside the function to ensure it's initialized
    from app.db import pool
    
    # Check if database pool is initialized
    if pool is None:
        raise HTTPException(status_code=500, detail="Database pool is not initialized. Please check server logs for database connection errors. Make sure PostgreSQL is running and the database exists.")

    service_id = service.id
    if not service.name:
        raise HTTPException(status_code=400, detail="Service 'name' is required")

    # Store service JSON in DB
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO services (id, name, description, basePrice, categoryName, tags, packages, attributes)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (id) DO UPDATE SET
                name=EXCLUDED.name,
                description=EXCLUDED.description,
                basePrice=EXCLUDED.basePrice,
                categoryName=EXCLUDED.categoryName,
                tags=EXCLUDED.tags,
                packages=EXCLUDED.packages,
                attributes=EXCLUDED.attributes
        """,
        service_id,
        service.name,
        service.description,
        service.basePrice,
        service.categoryName,
        json.dumps(service.tags or []),
        json.dumps([p.dict() for p in service.packages] or []),
        json.dumps([a.dict() for a in service.attributes] or [])
        )

    # Build a unified text containing all relevant service information
    packages_text = ""
    for p in service.packages:
        p_parts = [f"Package: {p.name}", f"Price: {p.price}", f"Description: {p.description}"]
        attr_text = []
        for a in p.attributes:
            val = a.stringValue or a.numberValue or a.booleanValue or a.dateValue or ""
            attr_text.append(f"{a.name}: {val}")
        if attr_text:
            p_parts.append(" | ".join(attr_text))
        packages_text += " | ".join(p_parts) + "\n"
    
    # Format service attributes
    service_attributes_text = ""
    for a in service.attributes:
        val = a.stringValue or a.numberValue or a.booleanValue or a.dateValue or ""
        service_attributes_text += f"{a.name}: {val}\n"

    full_text = f"""
Name: {service.name}
Description: {service.description}
Base Price: {service.basePrice}
Category: {service.categoryName}
Tags: {', '.join(service.tags or [])}
Packages:
{packages_text}
Service Attributes:
{service_attributes_text}
"""

    # Generate a single embedding for the entire service
    embedding = await embed_text(full_text)
    
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO service_embeddings (service_id, embedding)
            VALUES ($1,$2)
            ON CONFLICT (service_id) DO UPDATE SET embedding=EXCLUDED.embedding
        """, service_id, embedding)

    return {"status": "embedded", "service_id": service_id}