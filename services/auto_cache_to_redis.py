#!/usr/bin/env python3
"""
Automated PostgreSQL to Redis Caching Service
Reads data from PostgreSQL and caches it in Redis
"""

import os
import sys
import time
import json
import psycopg2
import redis
from datetime import datetime

# PostgreSQL configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mysecretpassword")

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Cache interval
CACHE_INTERVAL = int(os.getenv("CACHE_INTERVAL", "30"))  # seconds

def get_postgres_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def get_redis_connection():
    """Get Redis connection"""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )

def cache_content_table(redis_client, pg_conn):
    """Cache content table data"""
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id, slug, title, content_type, length_seconds, publish_ts FROM content ORDER BY publish_ts DESC LIMIT 50")
            rows = cur.fetchall()
            
            # Cache as JSON
            content_list = []
            for row in rows:
                content_item = {
                    "id": str(row[0]),
                    "slug": row[1],
                    "title": row[2],
                    "content_type": row[3],
                    "length_seconds": row[4],
                    "publish_ts": row[5].isoformat() if row[5] else None
                }
                content_list.append(content_item)
                
                # Cache individual items
                redis_client.setex(
                    f"content:{row[0]}",
                    3600,  # 1 hour TTL
                    json.dumps(content_item)
                )
            
            # Cache full list
            redis_client.setex(
                "content:list",
                3600,
                json.dumps(content_list)
            )
            
            # Cache count
            redis_client.setex("content:count", 3600, len(content_list))
            
            print(f"✅ Cached {len(content_list)} content items")
            return len(content_list)
    except Exception as e:
        print(f"❌ Error caching content: {e}")
        return 0

def cache_engagement_stats(redis_client, pg_conn):
    """Cache engagement statistics"""
    try:
        with pg_conn.cursor() as cur:
            # Total events
            cur.execute("SELECT COUNT(*) FROM engagement_events")
            total_events = cur.fetchone()[0]
            redis_client.setex("stats:total_events", 3600, total_events)
            
            # Events by type
            cur.execute("""
                SELECT event_type, COUNT(*) 
                FROM engagement_events 
                GROUP BY event_type
            """)
            for event_type, count in cur.fetchall():
                redis_client.setex(f"stats:events:{event_type}", 3600, count)
            
            # Recent events (last 20)
            cur.execute("""
                SELECT id, content_id, event_type, event_ts, duration_ms, device
                FROM engagement_events 
                ORDER BY event_ts DESC 
                LIMIT 20
            """)
            events = []
            for row in cur.fetchall():
                events.append({
                    "id": row[0],
                    "content_id": str(row[1]) if row[1] else None,
                    "event_type": row[2],
                    "event_ts": row[3].isoformat() if row[3] else None,
                    "duration_ms": row[4],
                    "device": row[5]
                })
            redis_client.setex("engagement:recent", 3600, json.dumps(events))
            
            print(f"✅ Cached engagement stats: {total_events} total events")
            return total_events
    except Exception as e:
        print(f"❌ Error caching engagement stats: {e}")
        return 0

def cache_analysis_results(redis_client, pg_conn):
    """Cache analysis results from analysis tables"""
    try:
        # Check if analysis tables exist
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE '%analysis%' OR table_name LIKE '%metrics%'
            """)
            tables = [row[0] for row in cur.fetchall()]
            
            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    redis_client.setex(f"analysis:{table}:count", 3600, count)
                    
                    # Cache summary stats if available
                    if table == "content_engagement_metrics":
                        cur.execute("""
                            SELECT 
                                AVG(total_events)::numeric(10,2),
                                AVG(engagement_score)::numeric(10,2),
                                SUM(total_events)
                            FROM content_engagement_metrics
                        """)
                        row = cur.fetchone()
                        if row:
                            redis_client.setex(
                                "analysis:metrics:summary",
                                3600,
                                json.dumps({
                                    "avg_events": float(row[0]) if row[0] else 0,
                                    "avg_engagement_score": float(row[1]) if row[1] else 0,
                                    "total_events": int(row[2]) if row[2] else 0
                                })
                            )
                except Exception as e:
                    print(f"⚠️  Could not cache {table}: {e}")
            
            print(f"✅ Cached analysis results from {len(tables)} tables")
            return len(tables)
    except Exception as e:
        print(f"❌ Error caching analysis results: {e}")
        return 0

def run_caching_cycle():
    """Run one caching cycle"""
    try:
        pg_conn = get_postgres_connection()
        redis_client = get_redis_connection()
        
        print("=" * 80)
        print("🔄 Starting caching cycle...")
        print("=" * 80)
        
        # Cache content
        content_count = cache_content_table(redis_client, pg_conn)
        
        # Cache engagement stats
        engagement_count = cache_engagement_stats(redis_client, pg_conn)
        
        # Cache analysis results
        analysis_tables = cache_analysis_results(redis_client, pg_conn)
        
        # Set last update timestamp
        redis_client.setex(
            "cache:last_update",
            3600,
            datetime.now().isoformat()
        )
        
        # Get total keys
        total_keys = redis_client.dbsize()
        
        print("=" * 80)
        print(f"✅ Caching complete!")
        print(f"   Content items: {content_count}")
        print(f"   Engagement events: {engagement_count}")
        print(f"   Analysis tables: {analysis_tables}")
        print(f"   Total Redis keys: {total_keys}")
        print("=" * 80)
        
        pg_conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error in caching cycle: {e}")
        import traceback
        traceback.print_exc()
        return False

def wait_for_services():
    """Wait for PostgreSQL and Redis to be ready"""
    import socket
    
    # Wait for PostgreSQL
    print("⏳ Waiting for PostgreSQL...")
    for i in range(30):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((POSTGRES_HOST, POSTGRES_PORT))
            sock.close()
            if result == 0:
                print("✅ PostgreSQL is ready")
                break
        except:
            pass
        time.sleep(2)
    else:
        print("❌ PostgreSQL not ready")
        return False
    
    # Wait for Redis
    print("⏳ Waiting for Redis...")
    for i in range(30):
        try:
            r = get_redis_connection()
            r.ping()
            print("✅ Redis is ready")
            break
        except:
            pass
        time.sleep(2)
    else:
        print("❌ Redis not ready")
        return False
    
    return True

def main():
    print("=" * 80)
    print("🚀 PostgreSQL to Redis Caching Service")
    print("=" * 80)
    print(f"Cache Interval: {CACHE_INTERVAL} seconds")
    print(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print("=" * 80)
    
    if not wait_for_services():
        print("❌ Services not ready, exiting")
        sys.exit(1)
    
    print("\n✅ All services ready!")
    time.sleep(2)
    
    cycle_count = 0
    try:
        while True:
            cycle_count += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{timestamp}] 🔄 Caching cycle #{cycle_count}")
            
            run_caching_cycle()
            
            print(f"\n⏳ Next cache update in {CACHE_INTERVAL} seconds...")
            time.sleep(CACHE_INTERVAL)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Caching service stopped after {cycle_count} cycles")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
