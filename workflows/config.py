import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class WorkflowConfig:
    """Configuration for workflow service"""
    
    # Temporal configuration
    TEMPORAL_SERVER_URL = os.getenv("TEMPORAL_SERVER_URL", "localhost:7233")
    TEMPORAL_TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "document-processing")
    TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
    
    # Worker configuration
    WORKER_MAX_CONCURRENT_ACTIVITIES = int(os.getenv("WORKER_MAX_CONCURRENT_ACTIVITIES", "10"))
    WORKER_MAX_CONCURRENT_WORKFLOWS = int(os.getenv("WORKER_MAX_CONCURRENT_WORKFLOWS", "5"))
    
    # Timeout configuration
    ACTIVITY_START_TO_CLOSE_TIMEOUT = int(os.getenv("ACTIVITY_START_TO_CLOSE_TIMEOUT", "600"))  # 10 minutes
    WORKFLOW_EXECUTION_TIMEOUT = int(os.getenv("WORKFLOW_EXECUTION_TIMEOUT", "3600"))  # 1 hour
    
    # Retry configuration
    MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
    INITIAL_RETRY_INTERVAL = int(os.getenv("INITIAL_RETRY_INTERVAL", "5"))  # seconds
    MAX_RETRY_INTERVAL = int(os.getenv("MAX_RETRY_INTERVAL", "300"))  # 5 minutes
    
    # Logging configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    @classmethod
    def get_retry_policy(cls):
        """Get retry policy configuration"""
        from temporalio.common import RetryPolicy
        from datetime import timedelta
        
        return RetryPolicy(
            initial_interval=timedelta(seconds=cls.INITIAL_RETRY_INTERVAL),
            maximum_interval=timedelta(seconds=cls.MAX_RETRY_INTERVAL),
            maximum_attempts=cls.MAX_RETRY_ATTEMPTS
        )
    
    @classmethod
    def get_activity_timeout(cls):
        """Get activity timeout configuration"""
        from datetime import timedelta
        return timedelta(seconds=cls.ACTIVITY_START_TO_CLOSE_TIMEOUT)
    
    @classmethod
    def get_workflow_timeout(cls):
        """Get workflow timeout configuration"""
        from datetime import timedelta
        return timedelta(seconds=cls.WORKFLOW_EXECUTION_TIMEOUT) 