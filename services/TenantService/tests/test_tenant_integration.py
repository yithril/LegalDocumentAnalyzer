import pytest
import os
import tempfile
from datetime import datetime

from tenant_service import TenantService
from models.tenant import Tenant

# Test configuration
TEST_TENANT_ID = "test-tenant-123"
TEST_TENANT_NAME = "Test Tenant Corporation"
TEST_PINECONE_INDEX = "test-tenant-index"
TEST_PINECONE_ENV = "us-east-1-aws"

@pytest.fixture
def tenant_service():
    """Create a TenantService instance for testing."""
    # Use test database URL - you can override this in your .env
    database_url = os.getenv("TEST_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/legal_document_analyzer")
    return TenantService(database_url)

@pytest.fixture
def sample_tenant_data():
    """Sample tenant data for testing."""
    return {
        "tenant_id": TEST_TENANT_ID,
        "name": TEST_TENANT_NAME,
        "pinecone_index_name": TEST_PINECONE_INDEX,
        "pinecone_environment": TEST_PINECONE_ENV,
        "status": "active",
        "metadata": {"test_key": "test_value"}
    }

def test_create_tenant_integration(tenant_service, sample_tenant_data):
    """Test creating a tenant with full validation."""
    print(f"Creating tenant: {sample_tenant_data['tenant_id']}")
    
    # Create tenant
    tenant = tenant_service.create_tenant(
        tenant_id=sample_tenant_data["tenant_id"],
        name=sample_tenant_data["name"],
        pinecone_index_name=sample_tenant_data["pinecone_index_name"],
        pinecone_environment=sample_tenant_data["pinecone_environment"],
        status=sample_tenant_data["status"],
        metadata=sample_tenant_data["metadata"]
    )
    
    # Verify tenant was created
    assert tenant is not None
    assert tenant.id == sample_tenant_data["tenant_id"]
    assert tenant.name == sample_tenant_data["name"]
    assert tenant.status == sample_tenant_data["status"]
    assert tenant.pinecone_index_name == sample_tenant_data["pinecone_index_name"]
    assert tenant.pinecone_environment == sample_tenant_data["pinecone_environment"]
    print("✓ Tenant created successfully")

def test_get_tenant_integration(tenant_service, sample_tenant_data):
    """Test retrieving a tenant from database."""
    print(f"Retrieving tenant: {sample_tenant_data['tenant_id']}")
    
    # Get tenant
    tenant = tenant_service.get_tenant(sample_tenant_data["tenant_id"])
    
    # Verify tenant was retrieved
    assert tenant is not None
    assert tenant.id == sample_tenant_data["tenant_id"]
    assert tenant.name == sample_tenant_data["name"]
    print("✓ Tenant retrieved successfully")

def test_update_tenant_integration(tenant_service, sample_tenant_data):
    """Test updating tenant information."""
    print(f"Updating tenant: {sample_tenant_data['tenant_id']}")
    
    # Update tenant
    updated_tenant = tenant_service.update_tenant(
        sample_tenant_data["tenant_id"],
        name="Updated Test Tenant",
        status="inactive"
    )
    
    # Verify tenant was updated
    assert updated_tenant is not None
    assert updated_tenant.name == "Updated Test Tenant"
    assert updated_tenant.status == "inactive"
    assert updated_tenant.pinecone_index_name == sample_tenant_data["pinecone_index_name"]  # Unchanged
    print("✓ Tenant updated successfully")

def test_tenant_status_operations_integration(tenant_service, sample_tenant_data):
    """Test tenant status operations (activate, deactivate, suspend)."""
    tenant_id = sample_tenant_data["tenant_id"]
    print(f"Testing status operations for tenant: {tenant_id}")
    
    # Test deactivate
    deactivated_tenant = tenant_service.deactivate_tenant(tenant_id)
    assert deactivated_tenant.status == "inactive"
    print("✓ Tenant deactivated successfully")
    
    # Test suspend
    suspended_tenant = tenant_service.suspend_tenant(tenant_id)
    assert suspended_tenant.status == "suspended"
    print("✓ Tenant suspended successfully")
    
    # Test activate
    activated_tenant = tenant_service.activate_tenant(tenant_id)
    assert activated_tenant.status == "active"
    print("✓ Tenant activated successfully")

def test_pinecone_config_integration(tenant_service, sample_tenant_data):
    """Test Pinecone configuration operations."""
    tenant_id = sample_tenant_data["tenant_id"]
    print(f"Testing Pinecone config for tenant: {tenant_id}")
    
    # Get Pinecone config
    config = tenant_service.get_tenant_pinecone_config(tenant_id)
    assert config is not None
    assert config["index_name"] == sample_tenant_data["pinecone_index_name"]
    assert config["environment"] == sample_tenant_data["pinecone_environment"]
    print("✓ Pinecone config retrieved successfully")
    
    # Update Pinecone config
    updated_tenant = tenant_service.update_tenant_pinecone_config(
        tenant_id, 
        "new-test-index", 
        "us-west1-gcp"
    )
    assert updated_tenant.pinecone_index_name == "new-test-index"
    assert updated_tenant.pinecone_environment == "us-west1-gcp"
    print("✓ Pinecone config updated successfully")

def test_tenant_exists_integration(tenant_service, sample_tenant_data):
    """Test tenant existence checking."""
    tenant_id = sample_tenant_data["tenant_id"]
    print(f"Testing tenant existence: {tenant_id}")
    
    # Check existing tenant
    exists = tenant_service.tenant_exists(tenant_id)
    assert exists is True
    print("✓ Tenant existence confirmed")
    
    # Check non-existing tenant
    not_exists = tenant_service.tenant_exists("non-existent-tenant")
    assert not_exists is False
    print("✓ Non-existent tenant correctly identified")

def test_get_all_tenants_integration(tenant_service):
    """Test retrieving all tenants with filtering."""
    print("Testing get all tenants")
    
    # Get all tenants
    all_tenants = tenant_service.get_all_tenants()
    assert isinstance(all_tenants, list)
    assert len(all_tenants) > 0
    print(f"✓ Retrieved {len(all_tenants)} tenants")
    
    # Get active tenants only
    active_tenants = tenant_service.get_active_tenants()
    assert isinstance(active_tenants, list)
    for tenant in active_tenants:
        assert tenant.status == "active"
    print(f"✓ Retrieved {len(active_tenants)} active tenants")

def test_validation_integration(tenant_service):
    """Test validation rules with invalid data."""
    print("Testing validation rules")
    
    # Test invalid tenant ID
    with pytest.raises(ValueError, match="Tenant ID must be 3-50 characters"):
        tenant_service.create_tenant(
            tenant_id="ab",  # Too short
            name="Test",
            pinecone_index_name="test-index",
            pinecone_environment="us-east-1-aws"
        )
    print("✓ Invalid tenant ID validation passed")
    
    # Test invalid status
    with pytest.raises(ValueError, match="Status must be one of"):
        tenant_service.create_tenant(
            tenant_id="test-tenant",
            name="Test",
            pinecone_index_name="test-index",
            pinecone_environment="us-east-1-aws",
            status="invalid-status"
        )
    print("✓ Invalid status validation passed")
    
    # Test invalid Pinecone index name
    with pytest.raises(ValueError, match="Pinecone index name must be"):
        tenant_service.create_tenant(
            tenant_id="test-tenant",
            name="Test",
            pinecone_index_name="INVALID-INDEX",  # Uppercase not allowed
            pinecone_environment="us-east-1-aws"
        )
    print("✓ Invalid Pinecone index validation passed")

def test_delete_tenant_integration(tenant_service, sample_tenant_data):
    """Test tenant deletion."""
    tenant_id = sample_tenant_data["tenant_id"]
    print(f"Testing tenant deletion: {tenant_id}")
    
    # Delete tenant
    deleted = tenant_service.delete_tenant(tenant_id)
    assert deleted is True
    print("✓ Tenant deleted successfully")
    
    # Verify tenant no longer exists
    exists = tenant_service.tenant_exists(tenant_id)
    assert exists is False
    print("✓ Tenant existence correctly updated after deletion")
    
    # Try to delete non-existent tenant
    not_deleted = tenant_service.delete_tenant("non-existent-tenant")
    assert not_deleted is False
    print("✓ Non-existent tenant deletion handled correctly")

def test_duplicate_tenant_integration(tenant_service, sample_tenant_data):
    """Test duplicate tenant creation."""
    print("Testing duplicate tenant creation")
    
    # Create first tenant
    tenant1 = tenant_service.create_tenant(
        tenant_id="duplicate-test",
        name="First Tenant",
        pinecone_index_name="first-index",
        pinecone_environment="us-east-1-aws"
    )
    assert tenant1 is not None
    print("✓ First tenant created successfully")
    
    # Try to create duplicate tenant
    with pytest.raises(ValueError, match="already exists"):
        tenant_service.create_tenant(
            tenant_id="duplicate-test",  # Same ID
            name="Second Tenant",
            pinecone_index_name="second-index",
            pinecone_environment="us-west1-gcp"
        )
    print("✓ Duplicate tenant creation properly rejected")
    
    # Clean up
    tenant_service.delete_tenant("duplicate-test")

if __name__ == "__main__":
    # Allow running the tests directly
    print("Running TenantService Integration Tests")
    print("=" * 60)
    print("Make sure you have:")
    print("1. PostgreSQL running (docker-compose up -d)")
    print("2. Database connection configured in .env")
    print("3. All dependencies installed (poetry install)")
    print()
    
    # Import pytest and run tests
    import pytest
    pytest.main([__file__, "-v"]) 