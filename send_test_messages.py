#!/usr/bin/env python3
"""
Test script to send sample messages to RabbitMQ queues for testing the consumer.
"""

import asyncio
import json
import aio_pika
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# RabbitMQ Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VIRTUAL_HOST = os.getenv("RABBITMQ_VIRTUAL_HOST", "/")

# Queue names
PRODUCT_QUEUE = os.getenv("PRODUCT_QUEUE", "product_queue")
SERVICE_QUEUE = os.getenv("SERVICE_QUEUE", "service_queue")

# Sample product data
sample_product = {
    "id": "prod_001",
    "name": "Wireless Headphones",
    "barcode": "1234567890123",
    "description": "High-quality wireless headphones with noise cancellation",
    "basePrice": 199.99,
    "categoryName": "Electronics",
    "brand": "TechBrand",
    "tags": ["wireless", "headphones", "electronics", "audio"],
    "variants": [
        {
            "id": "var_001",
            "sku": "WH-001-BLK",
            "price": 199.99,
            "stock": 50,
            "images": [],
            "attributes": [
                {
                    "id": "attr_001",
                    "templateId": None,
                    "name": "Color",
                    "dataType": "string",
                    "stringValue": "Black"
                },
                {
                    "id": "attr_002",
                    "templateId": None,
                    "name": "Battery Life",
                    "dataType": "string",
                    "stringValue": "30 hours"
                }
            ]
        },
        {
            "id": "var_002",
            "sku": "WH-001-WHT",
            "price": 199.99,
            "stock": 30,
            "images": [],
            "attributes": [
                {
                    "id": "attr_003",
                    "templateId": None,
                    "name": "Color",
                    "dataType": "string",
                    "stringValue": "White"
                },
                {
                    "id": "attr_004",
                    "templateId": None,
                    "name": "Battery Life",
                    "dataType": "string",
                    "stringValue": "30 hours"
                }
            ]
        }
    ],
    "attributes": [
        {
            "id": "attr_prod_001",
            "templateId": None,
            "name": "Weight",
            "dataType": "string",
            "stringValue": "250g"
        },
        {
            "id": "attr_prod_002",
            "templateId": None,
            "name": "Warranty",
            "dataType": "string",
            "stringValue": "2 years"
        }
    ]
}

# Sample service data
sample_service = {
    "id": "serv_001",
    "name": "Installation Service",
    "description": "Professional installation service for electronic devices",
    "basePrice": 49.99,
    "categoryName": "Services",
    "tags": ["installation", "professional", "electronics"],
    "packages": [
        {
            "id": "pkg_001",
            "name": "Basic Installation",
            "price": 49.99,
            "description": "Basic setup and installation of device",
            "images": [],
            "attributes": [
                {
                    "id": "attr_005",
                    "templateId": None,
                    "name": "Duration",
                    "dataType": "string",
                    "stringValue": "1-2 hours"
                },
                {
                    "id": "attr_006",
                    "templateId": None,
                    "name": "Support",
                    "dataType": "string",
                    "stringValue": "Email support for 30 days"
                }
            ]
        },
        {
            "id": "pkg_002",
            "name": "Premium Installation",
            "price": 89.99,
            "description": "Complete setup, installation, and training",
            "images": [],
            "attributes": [
                {
                    "id": "attr_007",
                    "templateId": None,
                    "name": "Duration",
                    "dataType": "string",
                    "stringValue": "2-3 hours"
                },
                {
                    "id": "attr_008",
                    "templateId": None,
                    "name": "Support",
                    "dataType": "string",
                    "stringValue": "Phone support for 90 days"
                }
            ]
        }
    ],
    "attributes": [
        {
            "id": "attr_serv_001",
            "templateId": None,
            "name": "Available Days",
            "dataType": "string",
            "stringValue": "Monday, Tuesday, Wednesday, Thursday, Friday"
        },
        {
            "id": "attr_serv_002",
            "templateId": None,
            "name": "Coverage Area",
            "dataType": "string",
            "stringValue": "Metro Area"
        }
    ]
}

async def send_test_messages():
    """Send test messages to RabbitMQ queues"""
    print("Connecting to RabbitMQ...")
    
    # Connect to RabbitMQ
    connection = await aio_pika.connect_robust(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        login=RABBITMQ_USERNAME,
        password=RABBITMQ_PASSWORD,
        virtualhost=RABBITMQ_VIRTUAL_HOST
    )
    
    channel = await connection.channel()
    
    # Declare queues to ensure they exist
    await channel.declare_queue(PRODUCT_QUEUE, durable=True)
    await channel.declare_queue(SERVICE_QUEUE, durable=True)
    
    print(f"Sending product message to queue '{PRODUCT_QUEUE}'...")
    
    # Send product message
    product_message = aio_pika.Message(
        body=json.dumps(sample_product).encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
    )
    
    await channel.default_exchange.publish(
        product_message,
        routing_key=PRODUCT_QUEUE
    )
    
    print(f"Sending service message to queue '{SERVICE_QUEUE}'...")
    
    # Send service message
    service_message = aio_pika.Message(
        body=json.dumps(sample_service).encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
    )
    
    await channel.default_exchange.publish(
        service_message,
        routing_key=SERVICE_QUEUE
    )
    
    # Close connection
    await connection.close()
    print("Test messages sent successfully!")

if __name__ == "__main__":
    asyncio.run(send_test_messages())