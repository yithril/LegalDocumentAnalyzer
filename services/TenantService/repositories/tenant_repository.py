from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from typing import List, Optional
import logging

from models.tenant import Tenant, Base

logger = logging.getLogger(__name__)

class TenantRepository:
    """Repository for tenant CRUD operations."""
    
    def __init__(self, database_url: str):
        """Initialize repository with database connection."""
        self.engine = create_engine(database_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Create tables if they don't exist
        Base.metadata.create_all(bind=self.engine)
    
    def _get_session(self) -> Session:
        """Get a database session."""
        return self.SessionLocal()
    
    def create_tenant(self, tenant: Tenant) -> Tenant:
        """Create a new tenant."""
        session = self._get_session()
        try:
            session.add(tenant)
            session.commit()
            session.refresh(tenant)
            logger.info(f"Created tenant: {tenant.id}")
            return tenant
        except IntegrityError as e:
            session.rollback()
            if "duplicate key" in str(e).lower():
                raise ValueError(f"Tenant with ID '{tenant.id}' already exists")
            raise
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error creating tenant: {e}")
            raise
        finally:
            session.close()
    
    def get_tenant_by_id(self, tenant_id: str) -> Optional[Tenant]:
        """Get tenant by ID."""
        session = self._get_session()
        try:
            tenant = session.query(Tenant).filter(Tenant.id == tenant_id).first()
            return tenant
        except SQLAlchemyError as e:
            logger.error(f"Database error getting tenant {tenant_id}: {e}")
            raise
        finally:
            session.close()
    
    def get_all_tenants(self, status: Optional[str] = None) -> List[Tenant]:
        """Get all tenants, optionally filtered by status."""
        session = self._get_session()
        try:
            query = session.query(Tenant)
            if status:
                query = query.filter(Tenant.status == status)
            return query.all()
        except SQLAlchemyError as e:
            logger.error(f"Database error getting tenants: {e}")
            raise
        finally:
            session.close()
    
    def update_tenant(self, tenant_id: str, **kwargs) -> Optional[Tenant]:
        """Update tenant by ID."""
        session = self._get_session()
        try:
            tenant = session.query(Tenant).filter(Tenant.id == tenant_id).first()
            if not tenant:
                return None
            
            # Validate updates if provided
            if 'id' in kwargs:
                raise ValueError("Cannot update tenant ID")
            
            if 'name' in kwargs:
                tenant._validate_name(kwargs['name'])
                tenant.name = kwargs['name']
            
            if 'status' in kwargs:
                tenant._validate_status(kwargs['status'])
                tenant.status = kwargs['status']
            
            if 'pinecone_index_name' in kwargs or 'pinecone_environment' in kwargs:
                index_name = kwargs.get('pinecone_index_name', tenant.pinecone_index_name)
                environment = kwargs.get('pinecone_environment', tenant.pinecone_environment)
                tenant._validate_pinecone_config(index_name, environment)
                tenant.pinecone_index_name = index_name
                tenant.pinecone_environment = environment
            
            if 'metadata' in kwargs:
                tenant.metadata = kwargs['metadata']
            
            session.commit()
            session.refresh(tenant)
            logger.info(f"Updated tenant: {tenant_id}")
            return tenant
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error updating tenant {tenant_id}: {e}")
            raise
        finally:
            session.close()
    
    def delete_tenant(self, tenant_id: str) -> bool:
        """Delete tenant by ID."""
        session = self._get_session()
        try:
            tenant = session.query(Tenant).filter(Tenant.id == tenant_id).first()
            if not tenant:
                return False
            
            session.delete(tenant)
            session.commit()
            logger.info(f"Deleted tenant: {tenant_id}")
            return True
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error deleting tenant {tenant_id}: {e}")
            raise
        finally:
            session.close()
    
    def tenant_exists(self, tenant_id: str) -> bool:
        """Check if tenant exists."""
        session = self._get_session()
        try:
            return session.query(Tenant).filter(Tenant.id == tenant_id).first() is not None
        except SQLAlchemyError as e:
            logger.error(f"Database error checking tenant existence {tenant_id}: {e}")
            raise
        finally:
            session.close()
    
    def get_active_tenants(self) -> List[Tenant]:
        """Get all active tenants."""
        return self.get_all_tenants(status='active') 