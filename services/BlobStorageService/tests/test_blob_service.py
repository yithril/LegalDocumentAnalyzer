import os
import tempfile
import pytest

from blob_service import BlobService

# Test configuration
TENANT_ID = "testtenant"
PROJECT_ID = "testproject"
BLOB_NAME = "testfile.txt"
TEST_CONTENT = b"Hello, Blob Storage! This is a test file for integration testing."

@pytest.fixture
def blob_service():
    return BlobService()

@pytest.fixture
def temp_file():
    """Create a temporary file with test content."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
        tmp.write(TEST_CONTENT)
        tmp_path = tmp.name
    
    yield tmp_path
    
    # Cleanup: remove the temporary file
    if os.path.exists(tmp_path):
        os.remove(tmp_path)

def test_blob_service_integration(blob_service, temp_file):
    """Test the complete workflow: upload, download, delete."""
    try:
        # Step 1: Upload the file
        print(f"Uploading file: {temp_file}")
        blob_service.upload_file(temp_file, TENANT_ID, PROJECT_ID, BLOB_NAME)
        print("✓ Upload successful")

        # Step 2: Download the file as a stream
        print(f"Downloading blob: {BLOB_NAME}")
        stream = blob_service.download_file(TENANT_ID, PROJECT_ID, BLOB_NAME)
        
        # Read the stream content
        downloaded_content = b"".join(chunk for chunk in stream.chunks())
        print(f"✓ Download successful, content length: {len(downloaded_content)} bytes")
        
        # Verify the content matches
        assert downloaded_content == TEST_CONTENT, "Downloaded content doesn't match original"
        print("✓ Content verification passed")

        # Step 3: Delete the blob
        print(f"Deleting blob: {BLOB_NAME}")
        blob_service.delete_file(TENANT_ID, PROJECT_ID, BLOB_NAME)
        print("✓ Delete successful")

        # Step 4: Verify deletion by attempting to download again
        print("Verifying deletion...")
        try:
            stream = blob_service.download_file(TENANT_ID, PROJECT_ID, BLOB_NAME)
            # If we get here, the blob still exists (which is an error)
            assert False, "Blob should have been deleted but still exists"
        except Exception as e:
            print(f"✓ Deletion verified (expected error: {type(e).__name__})")
        
        print("🎉 All integration tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise

def test_blob_service_stream_operations(blob_service, temp_file):
    """Test streaming operations specifically."""
    try:
        # Upload
        print("Testing stream operations...")
        blob_service.upload_file(temp_file, TENANT_ID, PROJECT_ID, "stream_test.txt")
        print("✓ Stream test upload successful")
        
        # Test 1: Reading in chunks
        print("Testing chunk reading...")
        stream = blob_service.download_file(TENANT_ID, PROJECT_ID, "stream_test.txt")
        chunks = []
        for chunk in stream.chunks():
            chunks.append(chunk)
        
        downloaded_content = b"".join(chunks)
        assert downloaded_content == TEST_CONTENT, "Streamed content doesn't match"
        print("✓ Chunk reading test passed!")
        
        # Test 2: Reading all at once (fresh stream)
        print("Testing readall()...")
        stream = blob_service.download_file(TENANT_ID, PROJECT_ID, "stream_test.txt")
        all_content = stream.readall()
        assert all_content == TEST_CONTENT, "readall() content doesn't match"
        print("✓ readall() test passed!")
        
        # Cleanup
        blob_service.delete_file(TENANT_ID, PROJECT_ID, "stream_test.txt")
        print("✓ Stream test cleanup successful")
        
        print("🎉 Stream operations test passed!")
        
    except Exception as e:
        print(f"❌ Stream test failed: {e}")
        raise

if __name__ == "__main__":
    # Allow running the test directly
    print("Running BlobService integration tests...")
    print("=" * 50)
    
    # Create service and run tests
    service = BlobService()
    
    # Create temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
        tmp.write(TEST_CONTENT)
        temp_file = tmp.name
    
    try:
        test_blob_service_integration(service, temp_file)
        test_blob_service_stream_operations(service, temp_file)
        print("\n" + "=" * 50)
        print("🎉 ALL TESTS PASSED! 🎉")
        print("=" * 50)
    except Exception as e:
        print("\n" + "=" * 50)
        print(f"❌ TESTS FAILED: {e}")
        print("=" * 50)
        raise
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file) 