from sqlalchemy.orm import Session
from models.document import Document
from sqlalchemy.exc import SQLAlchemyError

class DocumentRepository:
    def __init__(self, session: Session):
        self.session = session

    def add(self, document: Document):
        try:
            self.session.add(document)
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise e

    def get_by_id(self, document_id: str) -> Document:
        return self.session.query(Document).filter_by(id=document_id, is_active=True).first()

    def update(self, document: Document):
        try:
            self.session.merge(document)
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise e

    def soft_delete(self, document_id: str, updated_by: str):
        doc = self.get_by_id(document_id)
        if doc:
            doc.is_active = False
            doc.updated_by = updated_by
            from datetime import datetime
            doc.updated_at = datetime.utcnow()
            self.update(doc)

    def list_by_status(self, status: str = None, is_active: bool = True):
        query = self.session.query(Document)
        if is_active is not None:
            query = query.filter_by(is_active=is_active)
        if status:
            # Validate status before filtering
            valid_statuses = ['uploaded', 'processing', 'failed', 'completed']
            if status not in valid_statuses:
                raise ValueError(f"Invalid status: {status}. Must be one of {valid_statuses}")
            query = query.filter_by(status=status)
        return query.all() 