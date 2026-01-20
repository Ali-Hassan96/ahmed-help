#!/usr/bin/env python3
"""
Automated Spark Streaming Service
Runs the PostgreSQL to ClickHouse streaming job automatically
Waits for services to be ready, then starts streaming
"""

import os
import sys
import time
import subprocess
from datetime import datetime

# Configuration from environment variables
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8443")
STREAMING_INTERVAL = os.getenv("STREAMING_INTERVAL", "10")
SPARK_MASTER = "spark://spark-master:7077"
MAX_RETRIES = 30
RETRY_DELAY = 10  # seconds

def wait_for_service(host, port, service_name, max_retries=MAX_RETRIES):
    """Wait for a service to be ready"""
    import socket
    for i in range(max_retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                print(f"✅ {service_name} is ready")
                return True
        except Exception as e:
            pass
        print(f"⏳ Waiting for {service_name}... ({i+1}/{max_retries})")
        time.sleep(RETRY_DELAY)
    print(f"❌ {service_name} did not become ready in time")
    return False

def check_spark_master():
    """Check if Spark Master is ready"""
    return wait_for_service("spark-master", 7077, "Spark Master")

def check_postgres():
    """Check if PostgreSQL is ready"""
    return wait_for_service("postgres", 5432, "PostgreSQL")

def run_streaming_job():
    """Run the Spark streaming job"""
    if not CLICKHOUSE_HOST or not CLICKHOUSE_PASSWORD:
        print("⚠️  ClickHouse credentials not configured via environment variables")
        print("   Set CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD to enable streaming")
        print("   Streaming service will exit")
        return False
    
    print("=" * 80)
    print("🚀 Starting Automated Spark Streaming: PostgreSQL → ClickHouse")
    print("=" * 80)
    print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}")
    print(f"Streaming Interval: {STREAMING_INTERVAL} seconds")
    print("=" * 80)
    
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", SPARK_MASTER,
        "--packages", "org.postgresql:postgresql:42.7.1,com.clickhouse:clickhouse-jdbc:0.8.0",
        "/opt/spark/scripts/postgres_to_clickhouse_streaming.py"
    ]
    
    env = os.environ.copy()
    env.update({
        "CLICKHOUSE_HOST": CLICKHOUSE_HOST,
        "CLICKHOUSE_USER": CLICKHOUSE_USER,
        "CLICKHOUSE_PASSWORD": CLICKHOUSE_PASSWORD,
        "CLICKHOUSE_DATABASE": CLICKHOUSE_DATABASE,
        "CLICKHOUSE_PORT": CLICKHOUSE_PORT,
        "STREAMING_INTERVAL": STREAMING_INTERVAL
    })
    
    try:
        print(f"\n📅 Started at {datetime.now().isoformat()}")
        print("🔄 Streaming will continue until container stops...\n")
        
        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Stream output
        for line in process.stdout:
            print(line, end='')
        
        process.wait()
        return process.returncode == 0
        
    except KeyboardInterrupt:
        print("\n🛑 Streaming stopped")
        if process:
            process.terminate()
        return True
    except Exception as e:
        print(f"❌ Error running streaming job: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("=" * 80)
    print("🔧 Automated Spark Streaming Service")
    print("=" * 80)
    
    # Wait for dependencies
    print("\n📋 Checking dependencies...")
    if not check_postgres():
        print("❌ PostgreSQL not ready, exiting")
        sys.exit(1)
    
    if not check_spark_master():
        print("❌ Spark Master not ready, exiting")
        sys.exit(1)
    
    print("\n✅ All dependencies ready!")
    time.sleep(5)  # Give services a moment to fully initialize
    
    # Run streaming job
    success = run_streaming_job()
    
    if not success:
        print("\n❌ Streaming job failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
