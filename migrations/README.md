# Database Migrations

This directory contains database migration scripts for the Homez AI Search application.

## Prerequisites

Before running migrations, ensure that:

1. PostgreSQL with pgvector extension is installed
2. The database server is running
3. A database named `Homez_AI_Search` exists (see Database Creation section below)
4. The user has appropriate permissions to connect and modify the database
5. Environment variables are set (DATABASE_URL, POSTGRES_USER, etc.)

## Database Creation

Database creation is typically done outside of the migration system. You can create the database using one of these methods:

### Method 1: Using psql command line
```bash
# Connect to the default postgres database
psql -U postgres -d postgres

# Create the database
CREATE DATABASE Homez_AI_Search;
\q
```

### Method 2: Using Docker Compose (recommended)
The database is automatically created when you start the services with:
```bash
docker-compose up -d
```

This will create the database based on the environment variables in your `.env` file.

## Migration Files

Migrations are named with a numeric prefix followed by a descriptive name:
- `000_create_database.sql` - Documentation for database creation
- `001_create_migrations_table.sql` - Creates the migration tracking table
- `002_initial_schema.sql` - Creates the initial database schema
- `003_add_updated_at_trigger.sql` - Adds automatic updated_at timestamp triggers

## Running Migrations

To apply pending migrations to your database:

```bash
cd migrations
python migration_runner.py
```

The migration runner will:
1. Connect to the database using the DATABASE_URL environment variable
2. Check which migrations have already been applied
3. Apply any new migrations in numerical order
4. Record each applied migration in the schema_migrations table

## Migration Runner Features

- **Idempotent**: Migrations can be run multiple times safely
- **Ordered**: Migrations are applied in numerical order
- **Tracked**: Applied migrations are recorded to prevent re-application
- **Comment-aware**: SQL comments are stripped before execution

## Adding New Migrations

To add a new migration:
1. Create a new SQL file with the next sequential number (e.g., `004_add_user_table.sql`)
2. Include descriptive comments at the top of the file
3. Write your SQL statements using `IF NOT EXISTS` where appropriate to make migrations idempotent
4. Run the migration runner to apply the migration

## Migration Tracking

The `schema_migrations` table tracks which migrations have been applied:
- `version`: The numeric prefix of the migration file
- `name`: The full filename of the migration
- `applied_at`: Timestamp when the migration was applied

This ensures that migrations are only applied once and in the correct order.

## Handling Existing Databases

When migrating an existing database:
1. The migration system will detect which migrations have already been applied
2. Only new migrations will be applied
3. Existing tables and data will be preserved
4. New structures will be added incrementally