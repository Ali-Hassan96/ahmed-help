#!/usr/bin/env python3
"""
Automated PostgreSQL Analysis Service
Runs analysis periodically: reads from PostgreSQL, analyzes, writes results back
Waits for services to be ready, then starts automated analysis
"""

import os
import sys
import time
import subprocess
from datetime import datetime

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
ANALYSIS_INTERVAL = int(os.getenv("ANALYSIS_INTERVAL", "60"))  # seconds
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
        if i < max_retries - 1:
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

def run_analysis_job():
    """Run the Spark analysis job"""
    print("=" * 80)
    print("🔍 Running PostgreSQL Analysis Job")
    print("=" * 80)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", SPARK_MASTER,
        "--packages", "org.postgresql:postgresql:42.7.1",
        "/opt/spark/scripts/postgres_analyze_to_postgres.py"
    ]
    
    env = os.environ.copy()
    env.update({
        "POSTGRES_HOST": POSTGRES_HOST,
        "POSTGRES_PORT": str(POSTGRES_PORT),
        "POSTGRES_DB": os.getenv("POSTGRES_DB", "postgres"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "mysecretpassword")
    })
    
    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            print("✅ Analysis completed successfully")
            # Show last 20 lines of output
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                print("\n".join(lines[-20:]))
            return True
        else:
            print(f"❌ Analysis failed with code {result.returncode}")
            if result.stderr:
                print("Error output:")
                print(result.stderr[-500:])
            return False
            
    except subprocess.TimeoutExpired:
        print("⏱️  Analysis timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"❌ Error running analysis: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("=" * 80)
    print("🤖 Automated PostgreSQL Analysis Service")
    print("=" * 80)
    print(f"Analysis Interval: {ANALYSIS_INTERVAL} seconds")
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
    
    cycle_count = 0
    try:
        while True:
            cycle_count += 1
            print(f"\n{'='*80}")
            print(f"🔄 Analysis Cycle #{cycle_count}")
            print(f"{'='*80}\n")
            
            success = run_analysis_job()
            
            if success:
                print(f"\n✅ Cycle #{cycle_count} completed successfully")
            else:
                print(f"\n❌ Cycle #{cycle_count} failed")
            
            print(f"\n⏳ Next analysis in {ANALYSIS_INTERVAL} seconds...")
            time.sleep(ANALYSIS_INTERVAL)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Analysis service stopped after {cycle_count} cycles")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
