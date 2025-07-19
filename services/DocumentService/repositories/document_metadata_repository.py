from sqlalchemy.orm import Session
from models.document_metadata import DocumentMetadata
from sqlalchemy.exc import SQLAlchemyError

class DocumentMetadataRepository:
    def __init__(self, session: Session):
        self.session = session

    def add(self, metadata: DocumentMetadata):
        try:
            self.session.add(metadata)
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise e

    def update(self, metadata: DocumentMetadata):
        try:
            self.session.merge(metadata)
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise e

    def list_by_document_id(self, document_id: str):
        return (
            self.session.query(DocumentMetadata)
            .filter_by(document_id=document_id)
            .all()
        )

    def get_by_key(self, document_id: str, key: str):
        return (
            self.session.query(DocumentMetadata)
            .filter_by(document_id=document_id, key=key)
            .first()
        ) 