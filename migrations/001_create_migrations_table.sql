-- Migration: Create migrations tracking table
-- Date: 2025-12-19
-- Description: Create table to track applied migrations

CREATE TABLE IF NOT EXISTS schema_migrations (
    id SERIAL PRIMARY KEY,
    version VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP DEFAULT NOW()
);