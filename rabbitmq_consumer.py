#!/usr/bin/env python3
"""
Simple RabbitMQ consumer for products and services.
Connects to existing queues, consumes messages, processes them, and acknowledges.
"""

import asyncio
import json
import logging
import os
import sys
from typing import Dict, Any
import aio_pika
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from app.embedding_utils import embed_text
import asyncpg
from pgvector.asyncpg import register_vector

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VIRTUAL_HOST = os.getenv("RABBITMQ_VIRTUAL_HOST", "/")
RABBITMQ_PROTOCOL = os.getenv("RABBITMQ_PROTOCOL", "amqp")

PRODUCT_QUEUE = os.getenv("PRODUCT_QUEUE_NAME", "product_queue")
SERVICE_QUEUE = os.getenv("SERVICE_QUEUE_NAME", "service_queue")

# Global database pool and shutdown flag
db_pool = None
shutdown_event = asyncio.Event()


def convert_to_float(value):
    """Convert string to float, return 0 if conversion fails"""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def convert_to_int(value):
    """Convert string to int, return 0 if conversion fails"""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def get_attribute_value(attr):
    """Get attribute value and convert to appropriate type"""
    if attr.get('stringValue') is not None:
        return attr.get('stringValue')
    elif attr.get('numberValue') is not None:
        # Convert string number values to actual numbers
        num_val = attr.get('numberValue')
        if isinstance(num_val, str):
            try:
                # Try to convert to int first, then float
                if '.' in num_val:
                    return float(num_val)
                else:
                    return int(num_val)
            except (ValueError, TypeError):
                return num_val  # Return as string if conversion fails
        return num_val
    elif attr.get('booleanValue') is not None:
        return attr.get('booleanValue')
    elif attr.get('dateValue') is not None:
        return attr.get('dateValue')
    else:
        return ""


def fix_attribute_data_type(attr):
    """Fix attribute data types for storing in JSON"""
    attr_type = attr.get('type')
    if attr_type == 'NUMBER':
        return convert_to_float(attr.get('value'))
    elif attr_type == 'INTEGER':
        return convert_to_int(attr.get('value'))
    elif attr_type == 'BOOLEAN':
        return attr.get('value') in ['true', 'True', True]
    elif attr_type == 'DATE':
        return attr.get('value')
    else:
        return attr.get('value')


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
    # Check if product_data is valid
    if not isinstance(product_data, dict):
        logger.error(f"Invalid product data format: {type(product_data)}")
        return False
        
    product_id = product_data.get('id')
    if not product_id:
        logger.error(f"Product message missing 'id' field. Data: {product_data}")
        return False
        
    logger.info(f"Processing product: {product_id}")
    
    if not product_data.get('name'):
        logger.warning(f"Product {product_id} has no name, skipping")
        return False

    try:
        # Get database pool
        pool = await get_db_pool()
        
        # Check if product already exists
        async with pool.acquire() as conn:
            existing_product = await conn.fetchrow("SELECT id FROM products WHERE id = $1", product_id)
            
        is_update = existing_product is not None
        action = "Updating" if is_update else "Creating"
        logger.info(f"{action} product: {product_id}")
        
        # Fix attributes before storing
        fixed_variants = []
        for variant in product_data.get('variants', []):
            fixed_variant = variant.copy()
            if 'attributes' in fixed_variant and isinstance(fixed_variant['attributes'], list):
                fixed_variant['attributes'] = [fix_attribute_data_type(attr) for attr in fixed_variant['attributes']]
            fixed_variants.append(fixed_variant)
        
        fixed_attributes = [fix_attribute_data_type(attr) for attr in product_data.get('attributes', [])]
        
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
            convert_to_float(product_data.get('basePrice', 0)),
            product_data.get('categoryName', ''),
            product_data.get('brand'),
            json.dumps(product_data.get('tags', [])),
            json.dumps(fixed_variants),
            json.dumps(fixed_attributes)
            )
            logger.info(f"Product DB insert result: {result}")

        variants_text = ""
        for v in product_data.get('variants', []):
            v_parts = [f"SKU: {v.get('sku', '')}", f"Price: {convert_to_float(v.get('price', 0))}", f"Stock: {convert_to_int(v.get('stock', 0))}"]
            attr_text = []
            for a in v.get('attributes', []):
                val = get_attribute_value(a)
                attr_text.append(f"{a.get('name', '')}: {val}")
            if attr_text:
                v_parts.append(" | ".join(attr_text))
            variants_text += " | ".join(v_parts) + "\n"
        
        product_attributes_text = ""
        for a in product_data.get('attributes', []):
            val = get_attribute_value(a)
            product_attributes_text += f"{a.get('name', '')}: {val}\n"

        full_text = f"""
Name: {product_data.get('name', '')}
Description: {product_data.get('description', '')}
Base Price: {convert_to_float(product_data.get('basePrice', 0))}
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
            
        action_past = "Updated" if is_update else "Created"
        logger.info(f"✅ Successfully {action_past.lower()} product: {product_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Error processing product {product_id}: {e}", exc_info=True)
        return False


async def process_service_data(service_data: Dict[Any, Any]):
    """
    Process service data: store in DB and generate embeddings.
    This reuses your existing logic from bulk_import.py.
    """
    # Check if service_data is valid
    if not isinstance(service_data, dict):
        logger.error(f"Invalid service data format: {type(service_data)}")
        return False
        
    service_id = service_data.get('id')
    if not service_id:
        logger.error(f"Service message missing 'id' field. Data: {service_data}")
        return False
        
    logger.info(f"Processing service: {service_id}")
    
    if not service_data.get('name'):
        logger.warning(f"Service {service_id} has no name, skipping")
        return False

    try:
        # Get database pool
        pool = await get_db_pool()
        
        # Check if service already exists
        async with pool.acquire() as conn:
            existing_service = await conn.fetchrow("SELECT id FROM services WHERE id = $1", service_id)
            
        is_update = existing_service is not None
        action = "Updating" if is_update else "Creating"
        logger.info(f"{action} service: {service_id}")
        
        # Fix attributes before storing
        fixed_packages = []
        for package in service_data.get('packages', []):
            fixed_package = package.copy()
            if 'attributes' in fixed_package and isinstance(fixed_package['attributes'], list):
                fixed_package['attributes'] = [fix_attribute_data_type(attr) for attr in fixed_package['attributes']]
            fixed_packages.append(fixed_package)
        
        fixed_attributes = [fix_attribute_data_type(attr) for attr in service_data.get('attributes', [])]
        
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
            convert_to_float(service_data.get('basePrice', 0)),
            service_data.get('categoryName', ''),
            json.dumps(service_data.get('tags', [])),
            json.dumps(fixed_packages),
            json.dumps(fixed_attributes)
            )
            logger.info(f"Service DB insert result: {result}")

        packages_text = ""
        for p in service_data.get('packages', []):
            p_parts = [f"Package: {p.get('name', '')}", f"Price: {convert_to_float(p.get('price', 0))}", f"Description: {p.get('description', '')}"]
            attr_text = []
            for a in p.get('attributes', []):
                val = get_attribute_value(a)
                attr_text.append(f"{a.get('name', '')}: {val}")
            if attr_text:
                p_parts.append(" | ".join(attr_text))
            packages_text += " | ".join(p_parts) + "\n"
        
        service_attributes_text = ""
        for a in service_data.get('attributes', []):
            val = get_attribute_value(a)
            service_attributes_text += f"{a.get('name', '')}: {val}\n"

        full_text = f"""
Name: {service_data.get('name', '')}
Description: {service_data.get('description', '')}
Base Price: {convert_to_float(service_data.get('basePrice', 0))}
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
            
        action_past = "Updated" if is_update else "Created"
        logger.info(f"✅ Successfully {action_past.lower()} service: {service_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Error processing service {service_id}: {e}", exc_info=True)
        return False


async def process_product_message(message: aio_pika.IncomingMessage):
    """Process a product message from RabbitMQ"""
    try:
        # Log the raw message body for debugging
        raw_body = message.body.decode()
        logger.info(f"📥 Raw product message received: {raw_body}")
        
        # Parse the product data
        response = json.loads(raw_body)
        # product_data = response
        product_data = response.get('data', {})
        logger.info(f"📥 Parsed product message: {product_data.get('id', 'Unknown')}")

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
    except json.JSONDecodeError as e:
        logger.error(f"💥 Invalid JSON in product message: {e}")
        logger.info(f"Raw message body: {message.body.decode()}")
        # Reject and don't requeue invalid JSON messages
        await message.nack(requeue=False)
    except Exception as e:
        logger.error(f"💥 Error processing product message: {e}", exc_info=True)
        # Reject and requeue the message so it can be retried
        await message.nack(requeue=True)


async def process_service_message(message: aio_pika.IncomingMessage):
    """Process a service message from RabbitMQ"""
    try:
        # Log the raw message body for debugging
        raw_body = message.body.decode()
        logger.info(f"📥 Raw service message received: {raw_body}")
        
        # Parse the service data
        response = json.loads(raw_body)
        # service_data = response

        service_data = response.get('data', {})
        logger.info(f"📥 Parsed service message: {service_data.get('id', 'Unknown')}")
        
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
    except json.JSONDecodeError as e:
        logger.error(f"💥 Invalid JSON in service message: {e}")
        logger.info(f"Raw message body: {message.body.decode()}")
        # Reject and don't requeue invalid JSON messages
        await message.nack(requeue=False)
    except Exception as e:
        logger.error(f"💥 Error processing service message: {e}", exc_info=True)
        # Reject and requeue the message so it can be retried
        await message.nack(requeue=True)


async def consume_products():
    """Consume product messages from RabbitMQ"""
    logger.info(f"🔌 Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    
    # Connect to RabbitMQ
    if RABBITMQ_PROTOCOL == "amqps":
        connection = await aio_pika.connect_robust(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            login=RABBITMQ_USERNAME,
            password=RABBITMQ_PASSWORD,
            virtualhost=RABBITMQ_VIRTUAL_HOST,
            ssl=True
        )
    else:
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
    if RABBITMQ_PROTOCOL == "amqps":
        connection = await aio_pika.connect_robust(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            login=RABBITMQ_USERNAME,
            password=RABBITMQ_PASSWORD,
            virtualhost=RABBITMQ_VIRTUAL_HOST,
            ssl=True
        )
    else:
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
