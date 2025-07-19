import logging
from typing import List, Optional, Dict, Any
from models.tenant import Tenant
from repositories.tenant_repository import TenantRepository
from config import DATABASE_URL

logger = logging.getLogger(__name__)

class TenantService:
    """Main service for tenant management operations."""
    
    def __init__(self, database_url: str = None):
        """Initialize tenant service with database connection."""
        self.database_url = database_url or DATABASE_URL
        self.repository = TenantRepository(self.database_url)
        logger.info("TenantService initialized")
    
    def create_tenant(self, 
                     tenant_id: str, 
                     name: str, 
                     pinecone_index_name: str, 
                     pinecone_environment: str,
                     status: str = 'active',
                     metadata: Dict[str, Any] = None) -> Tenant:
        """Create a new tenant."""
        try:
            tenant = Tenant(
                id=tenant_id,
                name=name,
                status=status,
                pinecone_index_name=pinecone_index_name,
                pinecone_environment=pinecone_environment,
                metadata=str(metadata) if metadata else '{}'
            )
            
            return self.repository.create_tenant(tenant)
        except Exception as e:
            logger.error(f"Error creating tenant {tenant_id}: {e}")
            raise
    
    def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Get tenant by ID."""
        try:
            return self.repository.get_tenant_by_id(tenant_id)
        except Exception as e:
            logger.error(f"Error getting tenant {tenant_id}: {e}")
            raise
    
    def get_all_tenants(self, status: Optional[str] = None) -> List[Tenant]:
        """Get all tenants, optionally filtered by status."""
        try:
            return self.repository.get_all_tenants(status)
        except Exception as e:
            logger.error(f"Error getting tenants: {e}")
            raise
    
    def get_active_tenants(self) -> List[Tenant]:
        """Get all active tenants."""
        return self.get_all_tenants(status='active')
    
    def update_tenant(self, tenant_id: str, **kwargs) -> Optional[Tenant]:
        """Update tenant by ID."""
        try:
            return self.repository.update_tenant(tenant_id, **kwargs)
        except Exception as e:
            logger.error(f"Error updating tenant {tenant_id}: {e}")
            raise
    
    def delete_tenant(self, tenant_id: str) -> bool:
        """Delete tenant by ID."""
        try:
            return self.repository.delete_tenant(tenant_id)
        except Exception as e:
            logger.error(f"Error deleting tenant {tenant_id}: {e}")
            raise
    
    def tenant_exists(self, tenant_id: str) -> bool:
        """Check if tenant exists."""
        try:
            return self.repository.tenant_exists(tenant_id)
        except Exception as e:
            logger.error(f"Error checking tenant existence {tenant_id}: {e}")
            raise
    
    def activate_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Activate a tenant."""
        return self.update_tenant(tenant_id, status='active')
    
    def deactivate_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Deactivate a tenant."""
        return self.update_tenant(tenant_id, status='inactive')
    
    def suspend_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Suspend a tenant."""
        return self.update_tenant(tenant_id, status='suspended')
    
    def get_tenant_pinecone_config(self, tenant_id: str) -> Optional[Dict[str, str]]:
        """Get tenant's Pinecone configuration."""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return None
        
        return {
            'index_name': tenant.pinecone_index_name,
            'environment': tenant.pinecone_environment
        }
    
    def update_tenant_pinecone_config(self, tenant_id: str, index_name: str, environment: str) -> Optional[Tenant]:
        """Update tenant's Pinecone configuration."""
        return self.update_tenant(tenant_id, 
                                pinecone_index_name=index_name, 
                                pinecone_environment=environment) 