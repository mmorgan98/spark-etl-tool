from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from Job import Job

app = FastAPI()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this for security
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root endpoint
@app.get("/")
def test():
    return {"status": 200, "message": "API up and running."}

# GET Reqs
@app.get("/status/")
def get_job_status(job_name: str):
    status = "running"
    return {"status": 200, "message": f"Job {job_name} is {status}."}

@app.get("/jobs")
def get_jobs():
    job = Job("")
    return job.get_jobs()

@app.get("/jobs/spark")
def get_spark_options(job_name: str):
    job = Job(job_name)
    return job.get_spark_options()

@app.get("/jobs/step")
def get_job_steps(job_name: str):
    job = Job(job_name)
    return job.get_job_steps()

@app.get("/jobs/logging")
def get_logging_options(job_name: str):
    job = Job(job_name)
    return job.get_logging_options()

@app.get("/jobs/export")
def get_export_options(job_name: str):
    job = Job(job_name)
    return job.get_export_options()

# POST Reqs
@app.post("/jobs")
def create_job(job_name: str, job_description: str):
    new_job = Job(job_name)
    return new_job.setup_job(job_description)

@app.post("/jobs/step")
def add_job_step(job_name: str, step: dict):
    job = Job(job_name)
    return job.set_job_step(step)

@app.post("/jobs/spark")
def add_spark_option(job_name: str, option_key: str, option_val: str):
    job = Job(job_name)
    return job.set_spark_option(option_key, option_val)

@app.post("/jobs/logging")
def set_logging_options(job_name: str, logging_config: dict):
    job = Job(job_name)
    return job.set_logging_options(logging_config)

@app.post("/jobs/export")
def set_export_options(job_name: str, export_config: dict):
    job = Job(job_name)
    return job.set_export_option(export_config)

#DELETE Reqs
@app.delete("/jobs")
def delete_job(job_name: str):
    job = Job(job_name)
    return job.delete_job()

@app.delete("/jobs/step")
def delete_job_step(job_name: str, step_num: int):
    job = Job(job_name)
    return job.delete_job_step(step_num)

@app.delete("/jobs/spark")
def delete_spark_option(job_name: str, option_key: str):
    job = Job(job_name)
    return job.delete_spark_option(option_key)

@app.delete("/jobs/export")
def delete_export_option(job_name: str, option_num: int):
    job = Job(job_name)
    return job.delete_export_option(option_num)



# Run with: uvicorn filename:app --reload
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="localhost", port=8000, reload=True)