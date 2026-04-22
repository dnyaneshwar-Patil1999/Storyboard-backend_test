import threading
import uuid

class JobStore:
    _jobs = {}
    _lock = threading.Lock()

    @classmethod
    def create_job(cls):
        job_id = str(uuid.uuid4())
        with cls._lock:
            cls._jobs[job_id] = {"status": "processing", "result": None, "error": None}
        return job_id

    @classmethod
    def update_job(cls, job_id, update_data):
        """
        Updates a job with new data.
        
        Args:
            job_id: The ID of the job to update
            update_data: Dict containing update fields (status, message, progress, etc.)
        """
        with cls._lock:
            if job_id in cls._jobs:
                cls._jobs[job_id].update(update_data)

    @classmethod
    def set_result(cls, job_id, result):
        with cls._lock:
            if job_id in cls._jobs:
                cls._jobs[job_id]["status"] = "done"
                cls._jobs[job_id]["result"] = result

    @classmethod
    def set_error(cls, job_id, error):
        with cls._lock:
            if job_id in cls._jobs:
                cls._jobs[job_id]["status"] = "error"
                cls._jobs[job_id]["error"] = error

    @classmethod
    def get_job(cls, job_id):
        with cls._lock:
            return cls._jobs.get(job_id)