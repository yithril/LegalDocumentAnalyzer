from datetime import timedelta
from typing import Dict, Any, Optional
import logging
from temporalio import workflow
from temporalio.common import RetryPolicy
from dataclasses import dataclass

from shared.document_state_machine import DocumentState, DocumentStateMachine

logger = logging.getLogger(__name__)

@dataclass
class DocumentProcessingInput:
    """Input data for document processing workflow"""
    document_id: str
    tenant_id: str
    file_path: str
    file_name: str
    mime_type: str
    file_size: int
    created_by: str

@dataclass
class WorkflowState:
    """Workflow state for tracking progress"""
    document_id: str
    tenant_id: str
    current_state: DocumentState
    error_details: Optional[Dict[str, Any]] = None
    retry_count: int = 0
    max_retries: int = 3

@workflow.defn
class DocumentProcessingWorkflow:
    """Temporal workflow for orchestrating document processing pipeline"""
    
    @workflow.run
    async def run(self, input_data: DocumentProcessingInput) -> Dict[str, Any]:
        """
        Main workflow execution
        
        Args:
            input_data: Document processing input data
            
        Returns:
            Processing result with final status and metadata
        """
        # Initialize workflow state
        state = WorkflowState(
            document_id=input_data.document_id,
            tenant_id=input_data.tenant_id,
            current_state=DocumentState.UPLOADED
        )
        
        try:
            # Step 1: Text Extraction
            await self._extract_text(state, input_data)
            
            # Step 2: Chunking
            await self._chunk_document(state)
            
            # Step 3: Classification
            await self._classify_document(state)
            
            # Step 4: Vectorization
            await self._vectorize_document(state)
            
            # Step 5: Summarization
            await self._summarize_document(state)
            
            # Mark as completed
            await self._update_document_status(
                state.document_id, 
                DocumentState.COMPLETED.value
            )
            
            return {
                "status": "completed",
                "document_id": state.document_id,
                "final_state": DocumentState.COMPLETED.value
            }
            
        except Exception as e:
            # Handle workflow failure
            await self._handle_workflow_failure(state, str(e))
            raise
    
    async def _extract_text(self, state: WorkflowState, input_data: DocumentProcessingInput):
        """Extract text from document"""
        try:
            # Update state to extracting
            await self._update_document_status(
                state.document_id, 
                DocumentState.TEXT_EXTRACTING.value,
                step="text_extraction"
            )
            state.current_state = DocumentState.TEXT_EXTRACTING
            
            # Call text extraction activity
            extraction_result = await workflow.execute_activity(
                "extract_text_activity",
                {
                    "document_id": state.document_id,
                    "tenant_id": state.tenant_id,
                    "file_path": input_data.file_path,
                    "mime_type": input_data.mime_type
                },
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=5),
                    maximum_interval=timedelta(minutes=2),
                    maximum_attempts=3
                )
            )
            
            # Update state to extracted
            await self._update_document_status(
                state.document_id, 
                DocumentState.TEXT_EXTRACTED.value
            )
            state.current_state = DocumentState.TEXT_EXTRACTED
            
            logger.info(f"Text extraction completed for document {state.document_id}")
            
        except Exception as e:
            await self._handle_step_failure(state, "text_extraction", str(e))
            raise
    
    async def _chunk_document(self, state: WorkflowState):
        """Chunk the extracted text"""
        try:
            # Update state to chunking
            await self._update_document_status(
                state.document_id, 
                DocumentState.CHUNKING.value,
                step="chunking"
            )
            state.current_state = DocumentState.CHUNKING
            
            # Call chunking activity
            chunking_result = await workflow.execute_activity(
                "chunk_document_activity",
                {
                    "document_id": state.document_id,
                    "tenant_id": state.tenant_id
                },
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=5),
                    maximum_interval=timedelta(minutes=1),
                    maximum_attempts=3
                )
            )
            
            # Update state to chunked
            await self._update_document_status(
                state.document_id, 
                DocumentState.CHUNKED.value
            )
            state.current_state = DocumentState.CHUNKED
            
            logger.info(f"Chunking completed for document {state.document_id}")
            
        except Exception as e:
            await self._handle_step_failure(state, "chunking", str(e))
            raise
    
    async def _classify_document(self, state: WorkflowState):
        """Classify document based on chunks"""
        try:
            # Update state to classifying
            await self._update_document_status(
                state.document_id, 
                DocumentState.CLASSIFYING.value,
                step="classification"
            )
            state.current_state = DocumentState.CLASSIFYING
            
            # Call classification activity
            classification_result = await workflow.execute_activity(
                "classify_document_activity",
                {
                    "document_id": state.document_id,
                    "tenant_id": state.tenant_id
                },
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=5),
                    maximum_interval=timedelta(minutes=1),
                    maximum_attempts=3
                )
            )
            
            # Update state to classified
            await self._update_document_status(
                state.document_id, 
                DocumentState.CLASSIFIED.value
            )
            state.current_state = DocumentState.CLASSIFIED
            
            logger.info(f"Classification completed for document {state.document_id}")
            
        except Exception as e:
            await self._handle_step_failure(state, "classification", str(e))
            raise
    
    async def _vectorize_document(self, state: WorkflowState):
        """Vectorize document chunks"""
        try:
            # Update state to vectorizing
            await self._update_document_status(
                state.document_id, 
                DocumentState.VECTORIZING.value,
                step="vectorization"
            )
            state.current_state = DocumentState.VECTORIZING
            
            # Call vectorization activity
            vectorization_result = await workflow.execute_activity(
                "vectorize_document_activity",
                {
                    "document_id": state.document_id,
                    "tenant_id": state.tenant_id
                },
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=5),
                    maximum_interval=timedelta(minutes=2),
                    maximum_attempts=3
                )
            )
            
            # Update state to vectorized
            await self._update_document_status(
                state.document_id, 
                DocumentState.VECTORIZED.value
            )
            state.current_state = DocumentState.VECTORIZED
            
            logger.info(f"Vectorization completed for document {state.document_id}")
            
        except Exception as e:
            await self._handle_step_failure(state, "vectorization", str(e))
            raise
    
    async def _summarize_document(self, state: WorkflowState):
        """Summarize document"""
        try:
            # Update state to summarizing
            await self._update_document_status(
                state.document_id, 
                DocumentState.SUMMARIZING.value,
                step="summarization"
            )
            state.current_state = DocumentState.SUMMARIZING
            
            # Call summarization activity
            summarization_result = await workflow.execute_activity(
                "summarize_document_activity",
                {
                    "document_id": state.document_id,
                    "tenant_id": state.tenant_id
                },
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=5),
                    maximum_interval=timedelta(minutes=1),
                    maximum_attempts=3
                )
            )
            
            # Update state to summarized
            await self._update_document_status(
                state.document_id, 
                DocumentState.SUMMARIZED.value
            )
            state.current_state = DocumentState.SUMMARIZED
            
            logger.info(f"Summarization completed for document {state.document_id}")
            
        except Exception as e:
            await self._handle_step_failure(state, "summarization", str(e))
            raise
    
    async def _update_document_status(self, document_id: str, status: str, step: str = None):
        """Update document status in database"""
        await workflow.execute_activity(
            "update_document_status_activity",
            {
                "document_id": document_id,
                "status": status,
                "step": step
            },
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=3
            )
        )
    
    async def _handle_step_failure(self, state: WorkflowState, step: str, error_message: str):
        """Handle step failure and mark document as failed"""
        state.error_details = {
            "step": step,
            "error_type": "processing_error",
            "message": error_message,
            "retryable": True,
            "workflow_state": state.current_state.value
        }
        
        # Mark document as failed
        await workflow.execute_activity(
            "mark_document_failed_activity",
            {
                "document_id": state.document_id,
                "step": step,
                "error_type": "processing_error",
                "error_message": error_message,
                "retryable": True
            },
            start_to_close_timeout=timedelta(seconds=30)
        )
        
        # Update workflow state
        state.current_state = DocumentState.FAILED
        
        logger.error(f"Step {step} failed for document {state.document_id}: {error_message}")
    
    async def _handle_workflow_failure(self, state: WorkflowState, error_message: str):
        """Handle workflow-level failure"""
        state.error_details = {
            "step": "workflow",
            "error_type": "workflow_error",
            "message": error_message,
            "retryable": False,
            "workflow_state": state.current_state.value
        }
        
        # Mark document as failed
        await workflow.execute_activity(
            "mark_document_failed_activity",
            {
                "document_id": state.document_id,
                "step": "workflow",
                "error_type": "workflow_error",
                "error_message": error_message,
                "retryable": False
            },
            start_to_close_timeout=timedelta(seconds=30)
        )
        
        logger.error(f"Workflow failed for document {state.document_id}: {error_message}") 