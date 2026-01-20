#!/usr/bin/env python3
"""
Automated Data Seeding Service
Continuously seeds the database with sample data
Runs automatically when docker compose starts
"""

import psycopg2.extras
psycopg2.extras.register_uuid()
import psycopg2
import time
import uuid
import random
import sys
import os
from datetime import datetime, timezone

# Database configuration - use postgres hostname in Docker
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "mysecretpassword")
}

CONTENT_TYPES = ["podcast", "newsletter", "video"]
EVENT_TYPES = ["play", "pause", "finish", "click"]
DEVICES = ["ios", "android", "web-chrome", "web-safari", "web-edge"]

SEED_INTERVAL = int(os.getenv("SEED_INTERVAL", "30"))  # seconds
MAX_RETRIES = 30
RETRY_DELAY = 5

def wait_for_postgres():
    """Wait for PostgreSQL to be ready"""
    for i in range(MAX_RETRIES):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print("✅ PostgreSQL is ready")
            return True
        except Exception as e:
            if i < MAX_RETRIES - 1:
                print(f"⏳ Waiting for PostgreSQL... ({i+1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
            else:
                print(f"❌ PostgreSQL not ready: {e}")
                return False
    return False

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def seed_content(cur):
    content_id = uuid.uuid4()
    slug = f"auto-content-{content_id.hex[:8]}"

    cur.execute(
        """
        INSERT INTO content (
            id, slug, title, content_type, length_seconds, publish_ts
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (slug) DO NOTHING
        RETURNING id
        """,
        (
            content_id,
            slug,
            f"Auto Generated Content {slug[-4:]}",
            random.choice(CONTENT_TYPES),
            random.randint(300, 3600),
            datetime.now(timezone.utc)
        )
    )
    
    result = cur.fetchone()
    if result:
        return result[0]
    return None

def seed_engagement(cur, content_id):
    if not content_id:
        return
    
    event_type = random.choice(EVENT_TYPES)
    duration_ms = random.randint(1000, 300000) if event_type in ["play", "pause", "finish"] else None
    
    cur.execute(
        """
        INSERT INTO engagement_events (
            content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            content_id,
            uuid.uuid4(),
            event_type,
            datetime.now(timezone.utc),
            duration_ms,
            random.choice(DEVICES),
            f'{{"source":"auto-seed","timestamp":"{datetime.now(timezone.utc).isoformat()}"}}'
        )
    )

def run_seeding_cycle():
    """Run one seeding cycle"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Seed content
        content_id = seed_content(cur)
        
        # Seed engagement events (1-3 events per content)
        num_events = random.randint(1, 3)
        for _ in range(num_events):
            seed_engagement(cur, content_id)
        
        conn.commit()
        cur.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"❌ Error in seeding cycle: {e}")
        return False

def main():
    print("=" * 80)
    print("🌱 Automated Data Seeding Service")
    print("=" * 80)
    print(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    print(f"Seed Interval: {SEED_INTERVAL} seconds")
    print("=" * 80)
    
    # Wait for PostgreSQL
    print("\n📋 Waiting for PostgreSQL...")
    if not wait_for_postgres():
        print("❌ PostgreSQL not ready, exiting")
        sys.exit(1)
    
    print("\n✅ Starting continuous seeding...")
    print(f"   Will seed data every {SEED_INTERVAL} seconds")
    print("   Press Ctrl+C to stop\n")
    
    cycle_count = 0
    try:
        while True:
            cycle_count += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] 🔄 Seeding cycle #{cycle_count}...", end=" ")
            
            if run_seeding_cycle():
                print("✅ Success")
            else:
                print("❌ Failed")
            
            time.sleep(SEED_INTERVAL)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Seeding stopped after {cycle_count} cycles")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
