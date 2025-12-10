from fastapi import APIRouter, HTTPException
from app.embedding_utils import embed_text
from app.models import Product
import json

router = APIRouter()

@router.post("/")
async def ingest_product(product: Product):
    """
    Ingest a product at `/product` endpoint:
    - Store the product JSON as-is
    - Generate a single unified embedding from all product data
    - Store embedding using pgvector
    """
    
    # Import pool inside the function to ensure it's initialized
    from app.db import pool
    
    # Check if database pool is initialized
    if pool is None:
        raise HTTPException(status_code=500, detail="Database pool is not initialized. Please check server logs for database connection errors. Make sure PostgreSQL is running and the database exists.")

    product_id = product.id
    if not product.name:
        raise HTTPException(status_code=400, detail="Product 'name' is required")

    # Store product JSON in DB
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO products (id, name, barcode, description, basePrice, categoryName, brand, tags, variants, attributes)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (id) DO UPDATE SET
                name=EXCLUDED.name,
                barcode=EXCLUDED.barcode,
                description=EXCLUDED.description,
                basePrice=EXCLUDED.basePrice,
                categoryName=EXCLUDED.categoryName,
                brand=EXCLUDED.brand,
                tags=EXCLUDED.tags,
                variants=EXCLUDED.variants,
                attributes=EXCLUDED.attributes
        """,
        product_id,
        product.name,
        product.barcode,
        product.description,
        product.basePrice,
        product.categoryName,
        product.brand,
        json.dumps(product.tags or []),
        json.dumps([v.dict() for v in product.variants] or []),
        json.dumps([a.dict() for a in product.attributes] or [])
        )

    # Build a unified text containing all relevant product information
    variants_text = ""
    for v in product.variants:
        v_parts = [f"SKU: {v.sku}", f"Price: {v.price}", f"Stock: {v.stock}"]
        attr_text = []
        for a in v.attributes:
            val = a.stringValue or a.numberValue or a.booleanValue or a.dateValue or ""
            attr_text.append(f"{a.name}: {val}")
        if attr_text:
            v_parts.append(" | ".join(attr_text))
        variants_text += " | ".join(v_parts) + "\n"
    
    # Format product attributes
    product_attributes_text = ""
    for a in product.attributes:
        val = a.stringValue or a.numberValue or a.booleanValue or a.dateValue or ""
        product_attributes_text += f"{a.name}: {val}\n"

    full_text = f"""
Name: {product.name}
Description: {product.description}
Base Price: {product.basePrice}
Category: {product.categoryName}
Brand: {product.brand}
Tags: {', '.join(product.tags or [])}
Variants:
{variants_text}
Product Attributes:
{product_attributes_text}
"""

    # Generate a single embedding for the entire product
    embedding = await embed_text(full_text)
    
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO product_embeddings (product_id, embedding)
            VALUES ($1,$2)
            ON CONFLICT (product_id) DO UPDATE SET embedding=EXCLUDED.embedding
        """, product_id, embedding)

    return {"status": "embedded", "product_id": product_id}