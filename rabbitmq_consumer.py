#!/usr/bin/env python3
"""
Simple RabbitMQ consumer for products and services.
Connects to existing queues, consumes messages, processes them, and acknowledges.
"""

import asyncio
import json
import logging
import os
import signal
import sys
from typing import Dict, Any
import aio_pika
from dotenv import load_dotenv

load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from app.embedding_utils import embed_text
import asyncpg
from pgvector.asyncpg import register_vector

# RabbitMQ Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VIRTUAL_HOST = os.getenv("RABBITMQ_VIRTUAL_HOST", "/")

# Queue names
PRODUCT_QUEUE = os.getenv("PRODUCT_QUEUE", "product_queue")
SERVICE_QUEUE = os.getenv("SERVICE_QUEUE", "service_queue")

# Global database pool and shutdown flag
db_pool = None
shutdown_event = asyncio.Event()

async def get_db_pool():
    """Get or create database pool"""
    global db_pool
    if db_pool is None:
        # Import DATABASE_URL
        from app.db import DATABASE_URL
        
        logger.info("Creating database pool...")
        db_pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=1,
            max_size=10,
            init=register_vector  # Register pgvector extension
        )
        logger.info("✅ Database pool created")
    return db_pool

async def process_product_data(product_data: Dict[Any, Any]):
    """
    Process product data: store in DB and generate embeddings.
    This reuses your existing logic from bulk_import.py.
    """
    product_id = product_data['id']
    logger.info(f"Processing product: {product_id}")
    
    if not product_data.get('name'):
        logger.warning(f"Product {product_id} has no name, skipping")
        return False

    try:
        # Get database pool
        pool = await get_db_pool()
        
        # Store product JSON in DB
        async with pool.acquire() as conn:
            result = await conn.execute("""
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
            product_data.get('name', ''),
            product_data.get('barcode'),
            product_data.get('description', ''),
            product_data.get('basePrice', 0),
            product_data.get('categoryName', ''),
            product_data.get('brand'),
            json.dumps(product_data.get('tags', [])),
            json.dumps(product_data.get('variants', [])),
            json.dumps(product_data.get('attributes', []))
            )
            logger.info(f"Product DB insert result: {result}")

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
        
        product_attributes_text = ""
        for a in product_data.get('attributes', []):
            val = a.get('stringValue') or a.get('numberValue') or a.get('booleanValue') or a.get('dateValue') or ""
            product_attributes_text += f"{a.get('name', '')}: {val}\n"

        full_text = f"""
Name: {product_data.get('name', '')}
Description: {product_data.get('description', '')}
Base Price: {product_data.get('basePrice', 0)}
Category: {product_data.get('categoryName', '')}
Brand: {product_data.get('brand', '')}
Tags: {', '.join(product_data.get('tags', []))}
Variants:
{variants_text}
Product Attributes:
{product_attributes_text}
"""

        logger.info(f"Generating embedding for product {product_id} with text length: {len(full_text)}")
        
        embedding = await embed_text(full_text)
        logger.info(f"Generated embedding for product {product_id}, dimensions: {len(embedding) if embedding else 0}")
        
        async with pool.acquire() as conn:
            result = await conn.execute("""
                INSERT INTO product_embeddings (product_id, embedding)
                VALUES ($1,$2)
                ON CONFLICT (product_id) DO UPDATE SET embedding=EXCLUDED.embedding
            """, product_id, embedding)
            logger.info(f"Product embedding DB insert result: {result}")
            
        logger.info(f"✅ Successfully processed product: {product_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Error processing product {product_id}: {e}", exc_info=True)
        return False

async def process_service_data(service_data: Dict[Any, Any]):
    """
    Process service data: store in DB and generate embeddings.
    This reuses your existing logic from bulk_import.py.
    """
    service_id = service_data['id']
    logger.info(f"Processing service: {service_id}")
    
    if not service_data.get('name'):
        logger.warning(f"Service {service_id} has no name, skipping")
        return False

    try:
        # Get database pool
        pool = await get_db_pool()
        
        # Store service JSON in DB
        async with pool.acquire() as conn:
            result = await conn.execute("""
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
            service_data.get('name', ''),
            service_data.get('description', ''),
            service_data.get('basePrice', 0),
            service_data.get('categoryName', ''),
            json.dumps(service_data.get('tags', [])),
            json.dumps(service_data.get('packages', [])),
            json.dumps(service_data.get('attributes', []))
            )
            logger.info(f"Service DB insert result: {result}")

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
        
        service_attributes_text = ""
        for a in service_data.get('attributes', []):
            val = a.get('stringValue') or a.get('numberValue') or a.get('booleanValue') or a.get('dateValue') or ""
            service_attributes_text += f"{a.get('name', '')}: {val}\n"

        full_text = f"""
Name: {service_data.get('name', '')}
Description: {service_data.get('description', '')}
Base Price: {service_data.get('basePrice', 0)}
Category: {service_data.get('categoryName', '')}
Tags: {', '.join(service_data.get('tags', []))}
Packages:
{packages_text}
Service Attributes:
{service_attributes_text}
"""

        logger.info(f"Generating embedding for service {service_id} with text length: {len(full_text)}")
        
        embedding = await embed_text(full_text)
        logger.info(f"Generated embedding for service {service_id}, dimensions: {len(embedding) if embedding else 0}")
        
        async with pool.acquire() as conn:
            result = await conn.execute("""
                INSERT INTO service_embeddings (service_id, embedding)
                VALUES ($1,$2)
                ON CONFLICT (service_id) DO UPDATE SET embedding=EXCLUDED.embedding
            """, service_id, embedding)
            logger.info(f"Service embedding DB insert result: {result}")
            
        logger.info(f"✅ Successfully processed service: {service_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Error processing service {service_id}: {e}", exc_info=True)
        return False

async def process_product_message(message: aio_pika.IncomingMessage):
    """Process a product message from RabbitMQ"""
    try:
        # Parse the product data
        product_data = json.loads(message.body.decode())
        logger.info(f"📥 Received product message: {product_data.get('id', 'Unknown')}")
        
        # Process the product data
        success = await process_product_data(product_data)
        
        if success:
            # Acknowledge message after successful processing
            await message.ack()
            logger.info(f"✅ Acknowledged product message: {product_data.get('id')}")
        else:
            # Reject and requeue the message so it can be retried
            await message.nack(requeue=True)
            logger.warning(f"❌ Rejected product message (will retry): {product_data.get('id')}")
    except Exception as e:
        logger.error(f"💥 Error processing product message: {e}", exc_info=True)
        # Reject and requeue the message so it can be retried
        await message.nack(requeue=True)

async def process_service_message(message: aio_pika.IncomingMessage):
    """Process a service message from RabbitMQ"""
    try:
        # Parse the service data
        service_data = json.loads(message.body.decode())
        logger.info(f"📥 Received service message: {service_data.get('id', 'Unknown')}")
        
        # Process the service data
        success = await process_service_data(service_data)
        
        if success:
            # Acknowledge message after successful processing
            await message.ack()
            logger.info(f"✅ Acknowledged service message: {service_data.get('id')}")
        else:
            # Reject and requeue the message so it can be retried
            await message.nack(requeue=True)
            logger.warning(f"❌ Rejected service message (will retry): {service_data.get('id')}")
    except Exception as e:
        logger.error(f"💥 Error processing service message: {e}", exc_info=True)
        # Reject and requeue the message so it can be retried
        await message.nack(requeue=True)

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("🛑 Received shutdown signal")
    shutdown_event.set()

async def consume_products():
    """Consume product messages from RabbitMQ"""
    logger.info(f"🔌 Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    
    # Connect to RabbitMQ
    connection = await aio_pika.connect_robust(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        login=RABBITMQ_USERNAME,
        password=RABBITMQ_PASSWORD,
        virtualhost=RABBITMQ_VIRTUAL_HOST
    )
    
    channel = await connection.channel()
    
    # Declare queue (this will create it if it doesn't exist)
    queue = await channel.declare_queue(PRODUCT_QUEUE, durable=True)
    
    logger.info(f"📦 Product consumer started. Waiting for messages on queue '{PRODUCT_QUEUE}'...")
    
    # Set up consumer with manual acknowledgment
    await queue.consume(process_product_message, no_ack=False)
    
    # Wait for shutdown event
    await shutdown_event.wait()
    
    # Close connection
    await connection.close()
    logger.info("📦 Product consumer connection closed")

async def consume_services():
    """Consume service messages from RabbitMQ"""
    logger.info(f"🔌 Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    
    # Connect to RabbitMQ
    connection = await aio_pika.connect_robust(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        login=RABBITMQ_USERNAME,
        password=RABBITMQ_PASSWORD,
        virtualhost=RABBITMQ_VIRTUAL_HOST
    )
    
    channel = await connection.channel()
    
    # Declare queue (this will create it if it doesn't exist)
    queue = await channel.declare_queue(SERVICE_QUEUE, durable=True)
    
    logger.info(f"🛠️ Service consumer started. Waiting for messages on queue '{SERVICE_QUEUE}'...")
    
    # Set up consumer with manual acknowledgment
    await queue.consume(process_service_message, no_ack=False)
    
    # Wait for shutdown event
    await shutdown_event.wait()
    
    # Close connection
    await connection.close()
    logger.info("🛠️ Service consumer connection closed")

async def main():
    """Main function to run both consumers"""
    logger.info("🚀 Starting RabbitMQ Consumer")
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Initialize database connection pool
    try:
        await get_db_pool()
        logger.info("🗄️ Database connection pool initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize database connection pool: {e}", exc_info=True)
        return
    
    # Create tasks for both consumers
    logger.info("🔄 Starting consumer tasks...")
    product_task = asyncio.create_task(consume_products())
    service_task = asyncio.create_task(consume_services())
    
    logger.info("✅ Both consumers started. Press Ctrl+C to stop.")
    
    try:
        # Wait for both tasks (they run indefinitely until shutdown)
        await asyncio.gather(product_task, service_task)
    except asyncio.CancelledError:
        logger.info("🔄 Tasks cancelled")
    finally:
        logger.info("🛑 Consumer stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped by user")
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}", exc_info=True)
        sys.exit(1)