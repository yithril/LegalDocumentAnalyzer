from sqlalchemy.orm import Session
from models.document_history import DocumentHistory
from sqlalchemy.exc import SQLAlchemyError

class DocumentHistoryRepository:
    def __init__(self, session: Session):
        self.session = session

    def add(self, history: DocumentHistory):
        try:
            self.session.add(history)
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise e

    def list_by_document_id(self, document_id: str):
        return (
            self.session.query(DocumentHistory)
            .filter_by(document_id=document_id)
            .order_by(DocumentHistory.created_at)
            .all()
        ) 