from typing import Optional
import logging
import sys
import os

# Add service paths to sys.path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'services'))

from DocumentService.document_service import DocumentService
from DocumentProcessing.text_extraction_service import TextExtractionService
from DocumentProcessing.chunking_service import ChunkingService
from DocumentProcessing.document_classification_service import DocumentClassificationService
from VectorDbService.vector_service import VectorService
from BlobStorageService.blob_service import BlobStorageService
from TenantService.tenant_service import TenantService

logger = logging.getLogger(__name__)

class ServiceFactory:
    """Factory for creating and managing service instances"""
    
    def __init__(self):
        # Singleton instances for stateless services
        self._text_extraction_service: Optional[TextExtractionService] = None
        self._chunking_service: Optional[ChunkingService] = None
        self._classification_service: Optional[DocumentClassificationService] = None
        self._vector_service: Optional[VectorService] = None
        self._blob_service: Optional[BlobStorageService] = None
        self._tenant_service: Optional[TenantService] = None
        
        # Document services are created per-tenant (stateful)
        self._document_services: dict = {}
    
    def get_text_extraction_service(self) -> TextExtractionService:
        """Get or create text extraction service (singleton)"""
        if self._text_extraction_service is None:
            logger.info("Creating TextExtractionService instance")
            self._text_extraction_service = TextExtractionService()
        return self._text_extraction_service
    
    def get_chunking_service(self) -> ChunkingService:
        """Get or create chunking service (singleton)"""
        if self._chunking_service is None:
            logger.info("Creating ChunkingService instance")
            self._chunking_service = ChunkingService()
        return self._chunking_service
    
    def get_classification_service(self) -> DocumentClassificationService:
        """Get or create classification service (singleton)"""
        if self._classification_service is None:
            logger.info("Creating DocumentClassificationService instance")
            self._classification_service = DocumentClassificationService()
        return self._classification_service
    
    def get_vector_service(self) -> VectorService:
        """Get or create vector service (singleton)"""
        if self._vector_service is None:
            logger.info("Creating VectorService instance")
            self._vector_service = VectorService()
        return self._vector_service
    
    def get_blob_service(self) -> BlobStorageService:
        """Get or create blob service (singleton)"""
        if self._blob_service is None:
            logger.info("Creating BlobStorageService instance")
            self._blob_service = BlobStorageService()
        return self._blob_service
    
    def get_tenant_service(self) -> TenantService:
        """Get or create tenant service (singleton)"""
        if self._tenant_service is None:
            logger.info("Creating TenantService instance")
            self._tenant_service = TenantService()
        return self._tenant_service
    
    def get_document_service(self, tenant_id: str) -> DocumentService:
        """
        Get or create document service for specific tenant
        
        Args:
            tenant_id: Tenant identifier
            
        Returns:
            DocumentService instance for the tenant
        """
        if tenant_id not in self._document_services:
            logger.info(f"Creating DocumentService instance for tenant {tenant_id}")
            
            # Get tenant info to get database URL
            tenant_service = self.get_tenant_service()
            tenant_info = tenant_service.get_tenant(tenant_id)
            
            if not tenant_info:
                raise ValueError(f"Tenant {tenant_id} not found")
            
            # Create document service with tenant-specific database
            self._document_services[tenant_id] = DocumentService(tenant_info.database_url)
        
        return self._document_services[tenant_id]
    
    def create_document_service(self, database_url: str) -> DocumentService:
        """
        Create a new document service instance with specific database URL
        
        Args:
            database_url: Database connection URL
            
        Returns:
            New DocumentService instance
        """
        logger.info(f"Creating new DocumentService instance with database URL")
        return DocumentService(database_url)
    
    def clear_document_services(self):
        """Clear cached document services (useful for testing)"""
        self._document_services.clear()
        logger.info("Cleared cached document services")
    
    def get_service_summary(self) -> dict:
        """Get summary of created services for monitoring"""
        return {
            "text_extraction_service": self._text_extraction_service is not None,
            "chunking_service": self._chunking_service is not None,
            "classification_service": self._classification_service is not None,
            "vector_service": self._vector_service is not None,
            "blob_service": self._blob_service is not None,
            "tenant_service": self._tenant_service is not None,
            "document_services_count": len(self._document_services),
            "document_services_tenants": list(self._document_services.keys())
        }

# Global factory instance
_service_factory: Optional[ServiceFactory] = None

def get_service_factory() -> ServiceFactory:
    """Get the global service factory instance"""
    global _service_factory
    if _service_factory is None:
        _service_factory = ServiceFactory()
    return _service_factory

def reset_service_factory():
    """Reset the global service factory (useful for testing)"""
    global _service_factory
    _service_factory = None 