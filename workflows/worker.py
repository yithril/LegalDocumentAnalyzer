import asyncio
import logging
import os
from typing import Optional
from temporalio.client import Client
from temporalio.worker import Worker

from workflows.workflows.document_processing_workflow import DocumentProcessingWorkflow
from workflows.activities.document_activities import DocumentActivities
from workflows.factories.service_factory import get_service_factory

logger = logging.getLogger(__name__)

class DocumentProcessingWorker:
    """Temporal worker for document processing workflows and activities"""
    
    def __init__(self, temporal_server_url: str = "localhost:7233"):
        self.temporal_server_url = temporal_server_url
        self.client: Optional[Client] = None
        self.worker: Optional[Worker] = None
        
        # Get service factory for dependency injection
        self.service_factory = get_service_factory()
    
    async def initialize_services(self):
        """Initialize all services with proper configuration"""
        try:
            # Initialize services through factory
            self.service_factory.get_text_extraction_service()
            self.service_factory.get_chunking_service()
            self.service_factory.get_classification_service()
            self.service_factory.get_vector_service()
            self.service_factory.get_blob_service()
            self.service_factory.get_tenant_service()
            
            logger.info("All services initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {str(e)}")
            raise
    
    async def start(self, task_queue: str = "document-processing"):
        """Start the Temporal worker"""
        try:
            # Connect to Temporal server
            self.client = await Client.connect(self.temporal_server_url)
            logger.info(f"Connected to Temporal server at {self.temporal_server_url}")
            
            # Initialize services
            await self.initialize_services()
            
            # Create activities instance (factory handles dependency injection)
            activities = DocumentActivities()
            
            # Create worker
            self.worker = Worker(
                self.client,
                task_queue=task_queue,
                workflows=[DocumentProcessingWorkflow],
                activities=[
                    activities.extract_text_activity,
                    activities.chunk_document_activity,
                    activities.classify_document_activity,
                    activities.vectorize_document_activity,
                    activities.summarize_document_activity,
                    activities.update_document_status_activity,
                    activities.mark_document_failed_activity,
                ]
            )
            
            logger.info(f"Starting worker on task queue: {task_queue}")
            await self.worker.run()
            
        except Exception as e:
            logger.error(f"Failed to start worker: {str(e)}")
            raise
    
    async def stop(self):
        """Stop the worker"""
        if self.worker:
            await self.worker.shutdown()
            logger.info("Worker stopped")
    
    async def start_document_processing(self, input_data: dict) -> str:
        """Start a document processing workflow"""
        if not self.client:
            raise RuntimeError("Worker not started. Call start() first.")
        
        try:
            # Start workflow
            handle = await self.client.start_workflow(
                DocumentProcessingWorkflow.run,
                input_data,
                id=f"document-processing-{input_data['document_id']}",
                task_queue="document-processing"
            )
            
            logger.info(f"Started document processing workflow: {handle.id}")
            return handle.id
            
        except Exception as e:
            logger.error(f"Failed to start document processing workflow: {str(e)}")
            raise

async def main():
    """Main function to run the worker"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get configuration from environment
    temporal_server_url = os.getenv("TEMPORAL_SERVER_URL", "localhost:7233")
    task_queue = os.getenv("TEMPORAL_TASK_QUEUE", "document-processing")
    
    # Create and start worker
    worker = DocumentProcessingWorker(temporal_server_url)
    
    try:
        await worker.start(task_queue)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    finally:
        await worker.stop()

if __name__ == "__main__":
    asyncio.run(main()) 