from sqlalchemy.orm import Session
from typing import Optional, Dict, Any
import uuid
import logging
from models.chunk_metadata import ChunkMetadata

logger = logging.getLogger(__name__)

class ChunkMetadataRepository:
    """Repository for chunk metadata operations"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
    
    def create(self, metadata_data: Dict[str, Any]) -> ChunkMetadata:
        """
        Create a new chunk metadata record
        
        Args:
            metadata_data: Dictionary containing metadata fields
            
        Returns:
            Created ChunkMetadata instance
            
        Raises:
            ValueError: If validation fails
        """
        try:
            metadata = ChunkMetadata(**metadata_data)
            self.db_session.add(metadata)
            self.db_session.commit()
            
            logger.info(f"Created chunk metadata for document {metadata.document_id} with {metadata.chunk_count} chunks")
            return metadata
            
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Failed to create chunk metadata: {str(e)}")
            raise
    
    def get_by_document_id(self, document_id: uuid.UUID) -> Optional[ChunkMetadata]:
        """
        Get chunk metadata by document ID
        
        Args:
            document_id: Document UUID
            
        Returns:
            ChunkMetadata instance or None if not found
        """
        try:
            metadata = self.db_session.query(ChunkMetadata).filter(
                ChunkMetadata.document_id == document_id
            ).first()
            
            if metadata:
                logger.debug(f"Retrieved chunk metadata for document {document_id} with {metadata.chunk_count} chunks")
            else:
                logger.debug(f"No chunk metadata found for document {document_id}")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to retrieve chunk metadata for document {document_id}: {str(e)}")
            raise
    
    def delete(self, document_id: uuid.UUID) -> bool:
        """
        Delete chunk metadata
        
        Args:
            document_id: Document UUID
            
        Returns:
            True if deleted, False if not found
        """
        try:
            metadata = self.get_by_document_id(document_id)
            if not metadata:
                logger.warning(f"No chunk metadata found to delete for document {document_id}")
                return False
            
            self.db_session.delete(metadata)
            self.db_session.commit()
            
            logger.info(f"Deleted chunk metadata for document {document_id}")
            return True
            
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Failed to delete chunk metadata for document {document_id}: {str(e)}")
            raise 