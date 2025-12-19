-- Migration: Initial schema creation
-- Date: 2025-12-19
-- Description: Create initial database schema with products, services and embeddings tables

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    barcode TEXT,
    description TEXT,
    basePrice FLOAT,
    categoryName TEXT,
    brand TEXT,
    tags JSONB,
    variants JSONB,
    attributes JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create product embeddings table
CREATE TABLE IF NOT EXISTS product_embeddings (
    product_id TEXT REFERENCES products(id) ON DELETE CASCADE,
    embedding VECTOR(768),
    PRIMARY KEY(product_id)
);

-- Create index on product embeddings if it doesn't exist
-- Note: We can't use IF NOT EXISTS with CREATE INDEX in a clean way, so we'll skip this for now
-- CREATE INDEX ON product_embeddings USING ivfflat (embedding vector_cosine_ops);

-- Create services table
CREATE TABLE IF NOT EXISTS services (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    basePrice FLOAT,
    categoryName TEXT,
    tags JSONB,
    packages JSONB,
    attributes JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create service embeddings table
CREATE TABLE IF NOT EXISTS service_embeddings (
    service_id TEXT REFERENCES services(id) ON DELETE CASCADE,
    embedding VECTOR(768),
    PRIMARY KEY(service_id)
);

-- Create index on service embeddings if it doesn't exist
-- Note: We can't use IF NOT EXISTS with CREATE INDEX in a clean way, so we'll skip this for now
-- CREATE INDEX ON service_embeddings USING ivfflat (embedding vector_cosine_ops);