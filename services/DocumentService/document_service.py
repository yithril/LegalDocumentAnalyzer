from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from repositories.document_repository import DocumentRepository
from repositories.document_history_repository import DocumentHistoryRepository
from repositories.document_metadata_repository import DocumentMetadataRepository
from models.document import Document
from models.document_history import DocumentHistory
from models.document_metadata import DocumentMetadata
from sqlalchemy.exc import SQLAlchemyError

class DocumentService:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url, pool_pre_ping=True)
        self.Session = sessionmaker(bind=self.engine)

    def create_document(self, doc_data: dict):
        session = self.Session()
        try:
            repo = DocumentRepository(session)
            document = Document(**doc_data)
            repo.add(document)
            return document
        finally:
            session.close()

    def get_document(self, document_id: str):
        session = self.Session()
        try:
            repo = DocumentRepository(session)
            return repo.get_by_id(document_id)
        finally:
            session.close()

    def update_document(self, document: Document):
        session = self.Session()
        try:
            repo = DocumentRepository(session)
            repo.update(document)
        finally:
            session.close()

    def delete_document(self, document_id: str, updated_by: str):
        session = self.Session()
        try:
            repo = DocumentRepository(session)
            repo.soft_delete(document_id, updated_by)
        finally:
            session.close()

    def list_documents_by_status(self, status: str = None, is_active: bool = True):
        session = self.Session()
        try:
            repo = DocumentRepository(session)
            return repo.list_by_status(status=status, is_active=is_active)
        finally:
            session.close()

    def add_history(self, history_data: dict):
        session = self.Session()
        try:
            repo = DocumentHistoryRepository(session)
            history = DocumentHistory(**history_data)
            repo.add(history)
            return history
        finally:
            session.close()

    def list_history(self, document_id: str):
        session = self.Session()
        try:
            repo = DocumentHistoryRepository(session)
            return repo.list_by_document_id(document_id)
        finally:
            session.close()

    def add_metadata(self, metadata_data: dict):
        session = self.Session()
        try:
            repo = DocumentMetadataRepository(session)
            metadata = DocumentMetadata(**metadata_data)
            repo.add(metadata)
            return metadata
        finally:
            session.close()

    def update_metadata(self, metadata: DocumentMetadata):
        session = self.Session()
        try:
            repo = DocumentMetadataRepository(session)
            repo.update(metadata)
        finally:
            session.close()

    def list_metadata(self, document_id: str):
        session = self.Session()
        try:
            repo = DocumentMetadataRepository(session)
            return repo.list_by_document_id(document_id)
        finally:
            session.close()

    def get_metadata_by_key(self, document_id: str, key: str):
        session = self.Session()
        try:
            repo = DocumentMetadataRepository(session)
            return repo.get_by_key(document_id, key)
        finally:
            session.close()
    
    def update_document_status(self, document_id: str, status: str, step: str = None, error_details: dict = None):
        """Update document status and step"""
        session = self.Session()
        try:
            repo = DocumentRepository(session)
            document = repo.get_by_id(document_id)
            if document:
                document.update_status(status, step, error_details)
                repo.update(document)
                return document
            return None
        finally:
            session.close()
    
    def mark_document_failed(self, document_id: str, step: str, error_type: str, error_message: str, retryable: bool = True):
        """Mark document as failed with error details"""
        session = self.Session()
        try:
            repo = DocumentRepository(session)
            document = repo.get_by_id(document_id)
            if document:
                document.mark_failed(step, error_type, error_message, retryable)
                repo.update(document)
                return document
            return None
        finally:
            session.close() 