from typing import Dict, Any
import logging
from temporalio import activity

from ..factories.service_factory import get_service_factory

logger = logging.getLogger(__name__)

class DocumentActivities:
    """Temporal activities for document processing using factory pattern"""
    
    def __init__(self):
        # Get service factory for dependency injection
        self.service_factory = get_service_factory()
    
    @activity.defn
    async def extract_text_activity(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract text from document"""
        try:
            document_id = input_data["document_id"]
            tenant_id = input_data["tenant_id"]
            file_path = input_data["file_path"]
            mime_type = input_data["mime_type"]
            
            logger.info(f"Starting text extraction for document {document_id}")
            
            # Get services from factory
            tenant_service = self.service_factory.get_tenant_service()
            document_service = self.service_factory.get_document_service(tenant_id)
            text_extraction_service = self.service_factory.get_text_extraction_service()
            blob_service = self.service_factory.get_blob_service()
            
            # Get tenant info
            tenant_info = tenant_service.get_tenant(tenant_id)
            if not tenant_info:
                raise ValueError(f"Tenant {tenant_id} not found")
            
            # Extract text
            extraction_result = text_extraction_service.extract_text(file_path, mime_type)
            
            # Store extraction metadata
            metadata_data = {
                "document_id": document_id,
                "key": "text_extraction",
                "value": {
                    "extracted_text_length": len(extraction_result.extracted_text),
                    "file_path": file_path,
                    "mime_type": mime_type,
                    "extraction_method": extraction_result.extraction_method,
                    "processing_time": extraction_result.processing_time
                }
            }
            document_service.add_metadata(metadata_data)
            
            # Store extracted text in blob storage
            blob_path = f"documents/{document_id}/extracted_text.txt"
            blob_service.upload_text(blob_path, extraction_result.extracted_text)
            
            logger.info(f"Text extraction completed for document {document_id}")
            
            return {
                "status": "success",
                "extracted_text_length": len(extraction_result.extracted_text),
                "blob_path": blob_path
            }
            
        except Exception as e:
            logger.error(f"Text extraction failed for document {document_id}: {str(e)}")
            raise
    
    @activity.defn
    async def chunk_document_activity(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Chunk the extracted text"""
        try:
            document_id = input_data["document_id"]
            tenant_id = input_data["tenant_id"]
            
            logger.info(f"Starting chunking for document {document_id}")
            
            # Get services from factory
            tenant_service = self.service_factory.get_tenant_service()
            document_service = self.service_factory.get_document_service(tenant_id)
            chunking_service = self.service_factory.get_chunking_service()
            blob_service = self.service_factory.get_blob_service()
            
            # Get tenant info
            tenant_info = tenant_service.get_tenant(tenant_id)
            if not tenant_info:
                raise ValueError(f"Tenant {tenant_id} not found")
            
            # Get extracted text from blob storage
            blob_path = f"documents/{document_id}/extracted_text.txt"
            extracted_text = blob_service.download_text(blob_path)
            
            # Chunk the text
            chunking_result = chunking_service.chunk_text(extracted_text)
            
            # Store chunks in blob storage
            chunks_blob_path = f"documents/{document_id}/chunks.json"
            blob_service.upload_json(chunks_blob_path, chunking_result.chunks)
            
            # Store chunk metadata
            metadata_data = {
                "document_id": document_id,
                "key": "chunking",
                "value": {
                    "num_chunks": len(chunking_result.chunks),
                    "chunk_size": chunking_result.chunk_size,
                    "overlap_size": chunking_result.overlap_size,
                    "total_tokens": chunking_result.total_tokens,
                    "blob_path": chunks_blob_path
                }
            }
            document_service.add_metadata(metadata_data)
            
            logger.info(f"Chunking completed for document {document_id}: {len(chunking_result.chunks)} chunks")
            
            return {
                "status": "success",
                "num_chunks": len(chunking_result.chunks),
                "blob_path": chunks_blob_path
            }
            
        except Exception as e:
            logger.error(f"Chunking failed for document {document_id}: {str(e)}")
            raise
    
    @activity.defn
    async def classify_document_activity(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Classify document based on chunks"""
        try:
            document_id = input_data["document_id"]
            tenant_id = input_data["tenant_id"]
            
            logger.info(f"Starting classification for document {document_id}")
            
            # Get services from factory
            tenant_service = self.service_factory.get_tenant_service()
            document_service = self.service_factory.get_document_service(tenant_id)
            classification_service = self.service_factory.get_classification_service()
            blob_service = self.service_factory.get_blob_service()
            
            # Get tenant info
            tenant_info = tenant_service.get_tenant(tenant_id)
            if not tenant_info:
                raise ValueError(f"Tenant {tenant_id} not found")
            
            # Get chunks from blob storage
            chunks_blob_path = f"documents/{document_id}/chunks.json"
            chunks_data = blob_service.download_json(chunks_blob_path)
            chunks = [chunk["text"] for chunk in chunks_data]
            
            # Classify document
            classification_result = classification_service.classify_document(chunks)
            
            # Update document with classification result
            document = document_service.get_document(document_id)
            if document:
                document.document_type = classification_result.document_type
                document.confidence_score = classification_result.confidence_score
                document_service.update_document(document)
            
            # Store classification metadata
            metadata_data = {
                "document_id": document_id,
                "key": "classification",
                "value": {
                    "document_type": classification_result.document_type,
                    "confidence_score": classification_result.confidence_score,
                    "num_chunks_classified": len(chunks),
                    "classification_method": "bart-large-mnli"
                }
            }
            document_service.add_metadata(metadata_data)
            
            logger.info(f"Classification completed for document {document_id}: {classification_result.document_type}")
            
            return {
                "status": "success",
                "document_type": classification_result.document_type,
                "confidence_score": classification_result.confidence_score
            }
            
        except Exception as e:
            logger.error(f"Classification failed for document {document_id}: {str(e)}")
            raise
    
    @activity.defn
    async def vectorize_document_activity(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Vectorize document chunks"""
        try:
            document_id = input_data["document_id"]
            tenant_id = input_data["tenant_id"]
            
            logger.info(f"Starting vectorization for document {document_id}")
            
            # Get services from factory
            tenant_service = self.service_factory.get_tenant_service()
            document_service = self.service_factory.get_document_service(tenant_id)
            vector_service = self.service_factory.get_vector_service()
            blob_service = self.service_factory.get_blob_service()
            
            # Get tenant info
            tenant_info = tenant_service.get_tenant(tenant_id)
            if not tenant_info:
                raise ValueError(f"Tenant {tenant_id} not found")
            
            # Get chunks from blob storage
            chunks_blob_path = f"documents/{document_id}/chunks.json"
            chunks_data = blob_service.download_json(chunks_blob_path)
            
            # Vectorize chunks
            vectorization_result = vector_service.vectorize_chunks(
                chunks_data, 
                document_id, 
                tenant_info.pinecone_index_name,
                tenant_info.pinecone_region
            )
            
            # Store vectorization metadata
            metadata_data = {
                "document_id": document_id,
                "key": "vectorization",
                "value": {
                    "num_vectors_created": vectorization_result.num_vectors,
                    "vector_dimension": vectorization_result.vector_dimension,
                    "index_name": tenant_info.pinecone_index_name,
                    "processing_time": vectorization_result.processing_time
                }
            }
            document_service.add_metadata(metadata_data)
            
            logger.info(f"Vectorization completed for document {document_id}: {vectorization_result.num_vectors} vectors")
            
            return {
                "status": "success",
                "num_vectors": vectorization_result.num_vectors,
                "vector_dimension": vectorization_result.vector_dimension
            }
            
        except Exception as e:
            logger.error(f"Vectorization failed for document {document_id}: {str(e)}")
            raise
    
    @activity.defn
    async def summarize_document_activity(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize document"""
        try:
            document_id = input_data["document_id"]
            tenant_id = input_data["tenant_id"]
            
            logger.info(f"Starting summarization for document {document_id}")
            
            # Get services from factory
            tenant_service = self.service_factory.get_tenant_service()
            document_service = self.service_factory.get_document_service(tenant_id)
            blob_service = self.service_factory.get_blob_service()
            
            # Get tenant info
            tenant_info = tenant_service.get_tenant(tenant_id)
            if not tenant_info:
                raise ValueError(f"Tenant {tenant_id} not found")
            
            # Get extracted text from blob storage
            blob_path = f"documents/{document_id}/extracted_text.txt"
            extracted_text = blob_service.download_text(blob_path)
            
            # For now, create a simple summary (you can integrate with an LLM service later)
            summary = self._create_simple_summary(extracted_text)
            
            # Store summary in blob storage
            summary_blob_path = f"documents/{document_id}/summary.txt"
            blob_service.upload_text(summary_blob_path, summary)
            
            # Store summarization metadata
            metadata_data = {
                "document_id": document_id,
                "key": "summarization",
                "value": {
                    "summary_length": len(summary),
                    "original_text_length": len(extracted_text),
                    "compression_ratio": len(summary) / len(extracted_text) if extracted_text else 0,
                    "blob_path": summary_blob_path
                }
            }
            document_service.add_metadata(metadata_data)
            
            logger.info(f"Summarization completed for document {document_id}")
            
            return {
                "status": "success",
                "summary_length": len(summary),
                "blob_path": summary_blob_path
            }
            
        except Exception as e:
            logger.error(f"Summarization failed for document {document_id}: {str(e)}")
            raise
    
    @activity.defn
    async def update_document_status_activity(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update document status in database"""
        try:
            document_id = input_data["document_id"]
            status = input_data["status"]
            step = input_data.get("step")
            
            logger.info(f"Updating document {document_id} status to {status}")
            
            # Get document to find tenant
            tenant_service = self.service_factory.get_tenant_service()
            document = None
            tenant_id = None
            
            # Try to find the document in any tenant (for status updates)
            # This is a bit inefficient but necessary for status updates
            for tenant in tenant_service.list_tenants():
                try:
                    document_service = self.service_factory.get_document_service(tenant.id)
                    document = document_service.get_document(document_id)
                    if document:
                        tenant_id = tenant.id
                        break
                except:
                    continue
            
            if not document:
                raise ValueError(f"Document {document_id} not found in any tenant")
            
            # Update status
            document_service = self.service_factory.get_document_service(tenant_id)
            document_service.update_document_status(document_id, status, step)
            
            return {"status": "success"}
            
        except Exception as e:
            logger.error(f"Failed to update document status: {str(e)}")
            raise
    
    @activity.defn
    async def mark_document_failed_activity(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Mark document as failed with error details"""
        try:
            document_id = input_data["document_id"]
            step = input_data["step"]
            error_type = input_data["error_type"]
            error_message = input_data["error_message"]
            retryable = input_data.get("retryable", True)
            
            logger.error(f"Marking document {document_id} as failed at step {step}: {error_message}")
            
            # Get document to find tenant
            tenant_service = self.service_factory.get_tenant_service()
            document = None
            tenant_id = None
            
            # Try to find the document in any tenant
            for tenant in tenant_service.list_tenants():
                try:
                    document_service = self.service_factory.get_document_service(tenant.id)
                    document = document_service.get_document(document_id)
                    if document:
                        tenant_id = tenant.id
                        break
                except:
                    continue
            
            if not document:
                raise ValueError(f"Document {document_id} not found in any tenant")
            
            # Mark as failed
            document_service = self.service_factory.get_document_service(tenant_id)
            document_service.mark_document_failed(
                document_id, step, error_type, error_message, retryable
            )
            
            return {"status": "success"}
            
        except Exception as e:
            logger.error(f"Failed to mark document as failed: {str(e)}")
            raise
    
    def _create_simple_summary(self, text: str) -> str:
        """Create a simple summary of the text (placeholder for LLM integration)"""
        # This is a placeholder - you can integrate with OpenAI, Anthropic, etc.
        if len(text) <= 500:
            return text
        
        # Simple approach: take first 500 characters + "..."
        return text[:500] + "..." 