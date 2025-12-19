#!/usr/bin/env python3
"""
Reset migration tracking
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")

async def reset():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute('DELETE FROM schema_migrations')
        print('Migration tracking reset')
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(reset())