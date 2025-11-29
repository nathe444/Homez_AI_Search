import json
import asyncio
import os
import asyncpg
from app.db import init_db_pool, pool
from app.embedding_utils import embed_text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def insert_products_and_services(json_file_path):
    """Insert products and services from a JSON file"""
    
    # Initialize database connection
    await init_db_pool()
    
    # Check if pool is properly initialized
    from app.db import pool as db_pool
    if db_pool is None:
        raise RuntimeError("Database pool failed to initialize")
    
    # Read data from JSON file
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    products = data.get('products', [])
    services = data.get('services', [])
    
    print(f"Found {len(products)} products and {len(services)} services to import")
    
    # Insert products
    if products:
        print("Inserting products...")
        for product_data in products:
            try:
                await insert_product(product_data)
                print(f"  ✓ Inserted product: {product_data.get('name', 'Unknown')}")
            except Exception as e:
                print(f"  ✗ Failed to insert product {product_data.get('name', 'Unknown')}: {e}")
    
    # Insert services
    if services:
        print("Inserting services...")
        for service_data in services:
            try:
                await insert_service(service_data)
                print(f"  ✓ Inserted service: {service_data.get('name', 'Unknown')}")
            except Exception as e:
                print(f"  ✗ Failed to insert service {service_data.get('name', 'Unknown')}: {e}")
    
    print("Import completed!")

async def insert_product(product_data):
    """Insert a single product into the database with a single unified embedding"""
    product_id = product_data['id']
    
    # Check if pool is properly initialized
    from app.db import pool as db_pool
    if db_pool is None:
        raise RuntimeError("Database pool is not initialized")
    
    # Store product JSON in DB
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO products (id, name, barcode, description, basePrice, categoryName, brand, tags, variants, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (id) DO UPDATE SET
                name=EXCLUDED.name,
                barcode=EXCLUDED.barcode,
                description=EXCLUDED.description,
                basePrice=EXCLUDED.basePrice,
                categoryName=EXCLUDED.categoryName,
                brand=EXCLUDED.brand,
                tags=EXCLUDED.tags,
                variants=EXCLUDED.variants,
                metadata=EXCLUDED.metadata
        """,
        product_id,
        product_data.get('name', ''),
        product_data.get('barcode'),
        product_data.get('description', ''),
        product_data.get('basePrice', 0),
        product_data.get('categoryName', ''),
        product_data.get('brand'),
        json.dumps(product_data.get('tags', [])),
        json.dumps(product_data.get('variants', [])),
        json.dumps(product_data.get('metadata', {}))
        )
    
    # Build a unified text containing all relevant product information
    variants_text = ""
    for v in product_data.get('variants', []):
        v_parts = [f"SKU: {v.get('sku', '')}", f"Price: {v.get('price', 0)}", f"Stock: {v.get('stock', 0)}"]
        attr_text = []
        for a in v.get('attributes', []):
            val = a.get('stringValue') or a.get('numberValue') or a.get('booleanValue') or a.get('dateValue') or ""
            attr_text.append(f"{a.get('name', '')}: {val}")
        if attr_text:
            v_parts.append(" | ".join(attr_text))
        variants_text += " | ".join(v_parts) + "\n"
    
    full_text = f"""
Name: {product_data.get('name', '')}
Description: {product_data.get('description', '')}
Base Price: {product_data.get('basePrice', 0)}
Category: {product_data.get('categoryName', '')}
Brand: {product_data.get('brand', '')}
Tags: {', '.join(product_data.get('tags', []))}
Variants:
{variants_text}
Metadata: {json.dumps(product_data.get('metadata', {}))}
"""

    # Generate a single embedding for the entire product
    embedding = await embed_text(full_text)
    
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO product_embeddings (product_id, embedding)
            VALUES ($1, $2)
            ON CONFLICT (product_id) DO UPDATE SET embedding=EXCLUDED.embedding
        """, product_id, embedding)

async def insert_service(service_data):
    """Insert a single service into the database with a single unified embedding"""
    service_id = service_data['id']
    
    # Check if pool is properly initialized
    from app.db import pool as db_pool
    if db_pool is None:
        raise RuntimeError("Database pool is not initialized")
    
    # Store service JSON in DB
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO services (id, name, description, basePrice, categoryName, tags, packages, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (id) DO UPDATE SET
                name=EXCLUDED.name,
                description=EXCLUDED.description,
                basePrice=EXCLUDED.basePrice,
                categoryName=EXCLUDED.categoryName,
                tags=EXCLUDED.tags,
                packages=EXCLUDED.packages,
                metadata=EXCLUDED.metadata
        """,
        service_id,
        service_data.get('name', ''),
        service_data.get('description', ''),
        service_data.get('basePrice', 0),
        service_data.get('categoryName', ''),
        json.dumps(service_data.get('tags', [])),
        json.dumps(service_data.get('packages', [])),
        json.dumps(service_data.get('metadata', {}))
        )
    
    # Build a unified text containing all relevant service information
    packages_text = ""
    for p in service_data.get('packages', []):
        p_parts = [f"Package: {p.get('name', '')}", f"Price: {p.get('price', 0)}", f"Description: {p.get('description', '')}"]
        attr_text = []
        for a in p.get('attributes', []):
            val = a.get('stringValue') or a.get('numberValue') or a.get('booleanValue') or a.get('dateValue') or ""
            attr_text.append(f"{a.get('name', '')}: {val}")
        if attr_text:
            p_parts.append(" | ".join(attr_text))
        packages_text += " | ".join(p_parts) + "\n"
    
    full_text = f"""
Name: {service_data.get('name', '')}
Description: {service_data.get('description', '')}
Base Price: {service_data.get('basePrice', 0)}
Category: {service_data.get('categoryName', '')}
Tags: {', '.join(service_data.get('tags', []))}
Packages:
{packages_text}
Metadata: {json.dumps(service_data.get('metadata', {}))}
"""

    # Generate a single embedding for the entire service
    embedding = await embed_text(full_text)
    
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO service_embeddings (service_id, embedding)
            VALUES ($1, $2)
            ON CONFLICT (service_id) DO UPDATE SET embedding=EXCLUDED.embedding
        """, service_id, embedding)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python bulk_import.py <json_file_path>")
        print("Example: python bulk_import.py sample_data.json")
        sys.exit(1)
    
    json_file_path = sys.argv[1]
    
    if not os.path.exists(json_file_path):
        print(f"Error: File '{json_file_path}' not found.")
        sys.exit(1)
    
    # Run the import
    asyncio.run(insert_products_and_services(json_file_path))