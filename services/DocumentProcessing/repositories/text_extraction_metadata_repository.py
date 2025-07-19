from sqlalchemy.orm import Session
from typing import Optional, Dict, Any
import uuid
import logging
from models.text_extraction_metadata import TextExtractionMetadata

logger = logging.getLogger(__name__)

class TextExtractionMetadataRepository:
    """Repository for text extraction metadata operations"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
    
    def create(self, metadata_data: Dict[str, Any]) -> TextExtractionMetadata:
        """
        Create a new text extraction metadata record
        
        Args:
            metadata_data: Dictionary containing metadata fields
            
        Returns:
            Created TextExtractionMetadata instance
            
        Raises:
            ValueError: If validation fails
        """
        try:
            metadata = TextExtractionMetadata(**metadata_data)
            self.db_session.add(metadata)
            self.db_session.commit()
            
            logger.info(f"Created text extraction metadata for document {metadata.document_id}")
            return metadata
            
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Failed to create text extraction metadata: {str(e)}")
            raise
    
    def get_by_document_id(self, document_id: uuid.UUID) -> Optional[TextExtractionMetadata]:
        """
        Get text extraction metadata by document ID
        
        Args:
            document_id: Document UUID
            
        Returns:
            TextExtractionMetadata instance or None if not found
        """
        try:
            metadata = self.db_session.query(TextExtractionMetadata).filter(
                TextExtractionMetadata.document_id == document_id
            ).first()
            
            if metadata:
                logger.debug(f"Retrieved text extraction metadata for document {document_id}")
            else:
                logger.debug(f"No text extraction metadata found for document {document_id}")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to retrieve text extraction metadata for document {document_id}: {str(e)}")
            raise
    
    def delete(self, document_id: uuid.UUID) -> bool:
        """
        Delete text extraction metadata
        
        Args:
            document_id: Document UUID
            
        Returns:
            True if deleted, False if not found
        """
        try:
            metadata = self.get_by_document_id(document_id)
            if not metadata:
                logger.warning(f"No text extraction metadata found to delete for document {document_id}")
                return False
            
            self.db_session.delete(metadata)
            self.db_session.commit()
            
            logger.info(f"Deleted text extraction metadata for document {document_id}")
            return True
            
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Failed to delete text extraction metadata for document {document_id}: {str(e)}")
            raise 