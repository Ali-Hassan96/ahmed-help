#!/usr/bin/env python3
"""
Spark Job Scheduler - Runs Spark jobs automatically on a schedule
Similar to seed_loop.py and updated_at.py, but for Spark jobs
"""
import subprocess
import time
import sys
from datetime import datetime

SPARK_MASTER = "spark://spark-master:7077"
SPARK_SCRIPTS = [
    "/opt/spark/scripts/read_postgres.py"
]
JOB_INTERVAL = 60  # Run jobs every 60 seconds
PACKAGES = "org.postgresql:postgresql:42.7.1"

def run_spark_job(script_path):
    """Run a Spark job using spark-submit"""
    try:
        print(f"🚀 Starting Spark job: {script_path} at {datetime.utcnow().isoformat()}")
        
        cmd = [
            "/opt/spark/bin/spark-submit",
            "--master", SPARK_MASTER,
            "--packages", PACKAGES,
            script_path
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            print(f"✅ Spark job completed successfully")
            if result.stdout:
                print("Output:", result.stdout[-500:])  # Last 500 chars
        else:
            print(f"❌ Spark job failed with code {result.returncode}")
            if result.stderr:
                print("Error:", result.stderr[-500:])  # Last 500 chars
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print(f"⏱️ Spark job timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"❌ Error running Spark job: {e}")
        return False

def run():
    print("🚀 Spark Job Scheduler started")
    print(f"📋 Will run {len(SPARK_SCRIPTS)} job(s) every {JOB_INTERVAL} seconds")
    
    while True:
        try:
            for script in SPARK_SCRIPTS:
                run_spark_job(script)
                time.sleep(5)  # Small delay between jobs if multiple
            
        except KeyboardInterrupt:
            print("\n🛑 Spark scheduler stopped by user")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Scheduler error: {e}")
        
        time.sleep(JOB_INTERVAL)

if __name__ == "__main__":
    run()
