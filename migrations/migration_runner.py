#!/usr/bin/env python3
"""
Simple migration runner for applying SQL migrations
"""

import os
import asyncio
import asyncpg
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")

async def check_migration_applied(conn, version):
    """Check if a migration has already been applied"""
    try:
        result = await conn.fetchval(
            "SELECT COUNT(*) FROM schema_migrations WHERE version = $1",
            version
        )
        return result > 0
    except:
        # If the table doesn't exist yet, return False
        return False

async def record_migration_applied(conn, version, name):
    """Record that a migration has been applied"""
    await conn.execute(
        "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)",
        version, name
    )

async def apply_migration(conn, migration_file):
    """Apply a single migration file"""
    # Extract version from filename (e.g., 001_initial_schema.sql -> 001)
    version = os.path.basename(migration_file).split('_')[0]
    name = os.path.basename(migration_file)
    
    # Check if already applied
    if await check_migration_applied(conn, version):
        print(f"Migration {name} already applied, skipping...")
        return
    
    print(f"Applying migration: {name}")
    
    with open(migration_file, 'r') as f:
        sql = f.read()
    
    # Split SQL into statements by semicolon, but be careful with dollar quoting
    # Simple approach: split by semicolon and rejoin function definitions
    statements = []
    lines = sql.split('\n')
    current_statement = ""
    
    for line in lines:
        # Strip inline comments but preserve dollar quoted strings
        stripped_line = line.split('--')[0].strip()
        
        if stripped_line:
            current_statement += line + "\n"
            
            # Check if this line ends a statement
            if stripped_line.rstrip().endswith(';') and not '$$' in current_statement:
                statements.append(current_statement.strip())
                current_statement = ""
        
        # Special handling for $$ functions
        if '$$' in line and current_statement.count('$$') % 2 == 0:
            # We have a complete function definition
            statements.append(current_statement.strip())
            current_statement = ""
    
    # Add any remaining statement
    if current_statement.strip():
        statements.append(current_statement.strip())
    
    # Filter out empty statements
    statements = [stmt for stmt in statements if stmt.strip()]
    
    for statement in statements:
        # Skip completely empty statements
        if not statement.strip():
            continue
            
        try:
            await conn.execute(statement)
            # Print first line of statement for identification
            first_line = statement.strip().split('\n')[0]
            print(f"Executed: {first_line[:50]}...")
        except Exception as e:
            print(f"Error executing statement: {statement[:50]}...")
            print(f"Error: {e}")
            raise
    
    # Record that this migration was applied
    await record_migration_applied(conn, version, name)
    print(f"Migration {name} applied successfully!")

async def run_migrations():
    """Run all migrations in order"""
    print("Connecting to database...")
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Get list of migration files
        migrations_dir = os.path.dirname(__file__)
        migration_files = sorted([
            f for f in os.listdir(migrations_dir) 
            if f.endswith('.sql') and f != 'migration_runner.py'
        ])
        
        print(f"Found {len(migration_files)} migrations to check")
        
        # Apply each migration
        for migration_file in migration_files:
            migration_path = os.path.join(migrations_dir, migration_file)
            await apply_migration(conn, migration_path)
        
        print("Migration process completed!")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(run_migrations())