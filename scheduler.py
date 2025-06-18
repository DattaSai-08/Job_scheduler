import threading
import time
from datetime import datetime
from database import SessionLocal
from models import Job
import subprocess

def run_job(job):
    try:
        job.status = "running"
        job.last_run = datetime.utcnow()
        subprocess.run(job.command, shell=True, check=True)
        job.status = "completed"
    except Exception as e:
        job.status = "failed"

def job_runner():
    while True:
        time.sleep(5)
        db = SessionLocal()
        now = datetime.utcnow()
        jobs = db.query(Job).filter(Job.run_at <= now, Job.status == "pending").all()
        for job in jobs:
            run_job(job)
            db.commit()
        db.close()

def start_scheduler():
    thread = threading.Thread(target=job_runner, daemon=True)
    thread.start()
