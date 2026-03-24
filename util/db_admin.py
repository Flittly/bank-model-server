from pathlib import Path

import psycopg2
from psycopg2 import sql

import config


SCHEMA_SQL_PATH = (
    Path(__file__).resolve().parent.parent / "docs" / "database_schema.sql"
)


MIGRATION_SQL = """
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'bank_risk_results'
          AND column_name = 'task_id'
          AND data_type <> 'character varying'
    ) THEN
        ALTER TABLE bank_risk_results DROP CONSTRAINT IF EXISTS bank_risk_results_task_id_fkey;
        ALTER TABLE bank_risk_results ALTER COLUMN task_id TYPE VARCHAR(100) USING task_id::text;
        UPDATE bank_risk_results brr
        SET task_id = t.task_id
        FROM tasks t
        WHERE brr.task_id = t.id::text;
        ALTER TABLE bank_risk_results
            ADD CONSTRAINT bank_risk_results_task_id_fkey
            FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'bank_risk_results'
          AND column_name = 'section_id'
          AND data_type <> 'character varying'
    ) THEN
        ALTER TABLE bank_risk_results DROP CONSTRAINT IF EXISTS bank_risk_results_section_id_fkey;
        ALTER TABLE bank_risk_results ALTER COLUMN section_id TYPE VARCHAR(100) USING section_id::text;
        UPDATE bank_risk_results brr
        SET section_id = cs.section_id
        FROM cross_sections cs
        WHERE brr.section_id = cs.id::text;
        ALTER TABLE bank_risk_results
            ADD CONSTRAINT bank_risk_results_section_id_fkey
            FOREIGN KEY (section_id) REFERENCES cross_sections(section_id) ON DELETE CASCADE;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'cross_sections'
          AND column_name = 'task_id'
          AND data_type <> 'character varying'
    ) THEN
        ALTER TABLE cross_sections DROP CONSTRAINT IF EXISTS cross_sections_task_id_fkey;
        ALTER TABLE cross_sections ALTER COLUMN task_id TYPE VARCHAR(100) USING task_id::text;
        UPDATE cross_sections cs
        SET task_id = t.task_id
        FROM tasks t
        WHERE cs.task_id = t.id::text;
        ALTER TABLE cross_sections
            ADD CONSTRAINT cross_sections_task_id_fkey
            FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE;
    END IF;
END $$;

ALTER TABLE hydrodynamic_points
ADD COLUMN IF NOT EXISTS temp BOOLEAN DEFAULT FALSE NOT NULL;

CREATE INDEX IF NOT EXISTS idx_hydro_points_temp
ON hydrodynamic_points(temp);

-- 添加 section 验证字段
ALTER TABLE cross_sections
ADD COLUMN IF NOT EXISTS is_valid BOOLEAN DEFAULT NULL;

ALTER TABLE cross_sections
ADD COLUMN IF NOT EXISTS validation_status VARCHAR(50) DEFAULT 'pending';

ALTER TABLE cross_sections
ADD COLUMN IF NOT EXISTS validation_message TEXT;

-- 创建 tiff_bounds 表
CREATE TABLE IF NOT EXISTS tiff_bounds (
    id SERIAL PRIMARY KEY,
    tiff_key VARCHAR(255) UNIQUE NOT NULL,
    region_code VARCHAR(50),
    year VARCHAR(10),
    timepoint VARCHAR(20),
    min_x NUMERIC NOT NULL,
    min_y NUMERIC NOT NULL,
    max_x NUMERIC NOT NULL,
    max_y NUMERIC NOT NULL,
    geom GEOMETRY(Polygon, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tiff_bounds_key ON tiff_bounds(tiff_key);
CREATE INDEX IF NOT EXISTS idx_tiff_bounds_region ON tiff_bounds(region_code);
CREATE INDEX IF NOT EXISTS idx_tiff_bounds_geom ON tiff_bounds USING GIST(geom);
"""


def _ensure_database_exists() -> None:
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        dbname="postgres",
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                (config.DB_NAME,),
            )
            exists = cursor.fetchone()
            if not exists:
                print(f"Creating database {config.DB_NAME}...", flush=True)
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(config.DB_NAME))
                )
            else:
                print(f"Database {config.DB_NAME} already exists.", flush=True)
    finally:
        conn.close()


def _execute_sql(sql_text: str) -> None:
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        dbname=config.DB_NAME,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_text)
    finally:
        conn.close()


def init_db() -> None:
    """Initialize and migrate database schema for bank workflow."""
    _ensure_database_exists()

    if not SCHEMA_SQL_PATH.exists():
        raise FileNotFoundError(f"Schema file not found: {SCHEMA_SQL_PATH}")

    print("Applying base schema...", flush=True)

    sql_content = SCHEMA_SQL_PATH.read_text(encoding="utf-8-sig")

    lines = sql_content.splitlines()
    start_idx = 0
    for i, line in enumerate(lines):
        if not line.strip() or line.strip().startswith("--"):
            continue
        else:
            start_idx = i
            break

    clean_sql = "\n".join(lines[start_idx:])

    try:
        _execute_sql(clean_sql)
    except Exception as exc:
        print(f"Base schema apply warning: {exc}", flush=True)

    print("Applying compatibility migrations...", flush=True)
    if MIGRATION_SQL:
        _execute_sql(MIGRATION_SQL)
    else:
        print("  No migrations needed", flush=True)

    print("Database initialization completed.", flush=True)


def reset_database():
    """
    Reset database: drop all tables and recreate
    """
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            dbname=config.DB_NAME,
        )
        conn.autocommit = True
        cursor = conn.cursor()

        print("Dropping existing tables...")

        # IMPORTANT: Order matters (drop dependent tables first)
        tables = [
            "bank_risk_results",
            "cross_sections",
            "basic_params",
            "tasks",
            "banks",
            "correction_lines",
        ]

        for table in tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
                print(f"  ✓ Dropped {table}")
            except Exception as e:
                print(f"  ✗ Failed to drop {table}: {e}")

        print("\nRe-creating tables...")

        if SCHEMA_SQL_PATH.exists():
            sql_content = SCHEMA_SQL_PATH.read_text(encoding="utf-8-sig")
            cursor.execute(sql_content)
            print("  ✓ All tables created successfully")
        else:
            print(f"  ✗ Schema file not found: {SCHEMA_SQL_PATH}")

        cursor.close()
        conn.close()

        print("\n✓ Database reset completed successfully!")

    except Exception as e:
        print(f"✗ Error resetting database: {e}")
        raise


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "reset":
        print("=== Resetting Database ===")
        print("WARNING: This will delete all existing data!")
        response = input("Are you sure? (yes/no): ")
        if response.lower() in ["yes", "y"]:
            reset_database()
        else:
            print("Cancelled.")
    else:
        init_db()
