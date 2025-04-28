import os
import sys
from datetime import datetime
from pymongo import MongoClient
class Job:
    def __init__(self, job_name: str):
        self.job_name = job_name.upper()
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['etl']
        self.job_collection = self.db['jobs']

    def setup_job(self, description: str = "Default Job Description"):
        # Check if the job already exists
        existing_job = self.job_collection.find_one({'job_name': self.job_name})
        if existing_job:
            return {"status": 409, "message": f"Job {self.job_name} already exists."}

        spark_options = {
            'spark.master': 'local',
            'config': {
                'spark_dot_driver_dot_memory': '2g',
                'spark_dot_executor_dot_memory': '2g',
                'spark_dot_executor_dot_cores': '1'
            }
        }

        job_options = {
            'steps': []
        }

        export_options = {
            'export_list': []
        }

        logging_options = {
            'log_level': 'INFO'
        }
        # Create a new job entry
        job_entry = {
            'job_name': self.job_name,
            'job_description': description,
            'status': 'initialized',
            'created_at': datetime.now(),
            'modified_at': datetime.now(),
            'spark_config': spark_options,
            'job_config': job_options,
            'export_config': export_options,
            'logging_config': logging_options
        }
        
        # Insert the new job into the database
        result = self.job_collection.insert_one(job_entry)
        return {"status": 200, "message": f"Job {self.job_name} created with ID: {result.inserted_id}"}

    # Getters
    def get_jobs(self):
        jobs = self.job_collection.find()
        if jobs:
            return {"status": 200, "message": [{"job_name": job['job_name'], "description": job['job_description']} for job in jobs]}
        return {"status": 404, "message": "No jobs found."}

    def get_job_steps(self):
        job = self.job_collection.find_one({'job_name': self.job_name})
        if job:
            return {"status": 200, "message": job.get('job_config', {})}
        return {"status": 404, "message": f"Job {self.job_name} not found."}
    
    def get_spark_options(self):
        job = self.job_collection.find_one({'job_name': self.job_name})
        if job:
            spark_config = dict(job.get('spark_config', {}))
            for key in list(spark_config['config'].keys()):
                spark_config['config'][key.replace('_dot_','.')] = spark_config['config'].pop(key)
            return {"status": 200, "message": spark_config}
        return {"status": 404, "message": f"Job {self.job_name} not found."}

    def get_export_options(self):
        job = self.job_collection.find_one({'job_name': self.job_name})
        if job:
            return {"status": 200, "message": job.get('export_config', {})}
        return {"status": 404, "message": f"Job {self.job_name} not found."}

    def get_logging_options(self):
        job = self.job_collection.find_one({'job_name': self.job_name})
        if job:
            return {"status": 200, "message": job.get('logging_config', {})}
        return {"status": 404, "message": f"Job {self.job_name} not found."}
    
    # Setters
    def set_job_step(self, step: dict):
        result = self.job_collection.update_one(
            {'job_name': self.job_name},
            {'$push': {'job_config.steps': step}}
        )
        if result.modified_count > 0:
            return {"status": 200, "message": f"Step added to job {self.job_name}."}
        return {"status": 404, "message": f"Job {self.job_name} not found."}

    def set_spark_option(self, option_key: str, option_val: str):
        result = self.job_collection.update_one(
            {'job_name': self.job_name},
            {'$set': {f'spark_config.config.{option_key.replace(".", "_dot_")}': option_val}}
        )
        if result.modified_count > 0:
            return {"status": 200, "message": f"Spark options updated for job {self.job_name}."}
        return {"status": 404, "message": f"Job {self.job_name} not found."}
    
    def set_export_option(self, option: dict):
        result = self.job_collection.update_one(
            {'job_name': self.job_name},
            {'$push': {'export_config.export_list': option}}
        )
        if result.modified_count > 0:
            return {"status": 200, "message": f"Export option added to job {self.job_name}."}
        return {"status": 404, "message": f"Job {self.job_name} not found."}
    
    def set_logging_options(self, options: dict):
        result = self.job_collection.update_one(
            {'job_name': self.job_name},
            {'$set': {'logging_config': options}}
        )
        if result.modified_count > 0:
            return {"status": 200, "message": f"Logging options updated for job {self.job_name}."}
        return {"status": 404, "message": f"Job {self.job_name} not found."}
    
    # Deletes
    def delete_job(self):
        result = self.job_collection.delete_one({'job_name': self.job_name})
        if result.deleted_count > 0:
            return {"status": 200, "message": f"Job {self.job_name} deleted."}
        return {"status": 404, "message": f"Job {self.job_name} not found."}

    def delete_job_step(self, step_num: int):
        result = self.job_collection.update_one(
            {'job_name': self.job_name},
            {'$unset': {f'job_config.steps.{step_num-1}': 1}}
        )
        if result.modified_count > 0:
            self.job_collection.update_one(
                {'job_name': self.job_name},
                {"$pull": {'job_config.steps': None}}
            )
            return {"status": 200, "message": f"Step {step_num} deleted from job {self.job_name}."}
        return {"status": 404, "message": f"Job {self.job_name} not found."}

    def delete_spark_option(self, option_key: str):
        result = self.job_collection.update_one(
            {'job_name': self.job_name},
            {'$unset': {f'spark_config.config.{option_key.replace(".", "_dot_")}': 1}}
        )
        if result.modified_count > 0:     
            return {"status": 200, "message": f"Spark option {option_key} deleted from job {self.job_name}."}
        return {"status": 404, "message": f"Job {self.job_name} not found."}
    
    def delete_export_option(self, option_num: int):
        result = self.job_collection.update_one(
            {'job_name': self.job_name},
            {'$unset': {f'export_config.export_list.{option_num-1}': 1}}
        )
        if result.modified_count > 0:
            self.job_collection.update_one(
                {'job_name': self.job_name},
                {"$pull": {'export_config.export_list': None}}
            )
            return {"status": 200, "message": f"Export option {option_num} deleted from job {self.job_name}."}
        return {"status": 404, "message": f"Job {self.job_name} not found."}
        

if __name__ == "__main__":
    # Example usage
    job_name = sys.argv[1] if len(sys.argv) > 1 else 'DEFAULT_JOB'
    job = Job(job_name)
    
    job.delete_job()
    job.setup_job()
    job.set_job_step({'step_name': 'Extract', 'step_type': 'extract'})
    job.set_job_step({'step_name': 'Transform', 'step_type': 'transform'})
    job.set_job_step({'step_name': 'Load', 'step_type': 'load'})
    job.set_spark_option('spark.executor.memory', '4g')
    job.set_spark_option('spark.driver.memory', '8g')
    job.set_export_option({'export_type': 'csv', 'path': '/data/output'})
    job.set_logging_option({'log_level': 'DEBUG'})

    
    print(job.delete_job_step(1))
    print(job.delete_spark_option('spark.executor.memory'))
    print(job.delete_export_option(1))
    print(job.get_jobs())
    print(job.get_job_steps())
    print(job.get_spark_options())
    print(job.get_export_options())
    print(job.get_logging_options())
    