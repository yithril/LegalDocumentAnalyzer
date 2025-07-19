import logging
import sys
import os

# Add the parent directory to the path so we can import the service
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chunking_service import ChunkingService, EmptyTextError, ChunkingError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_chunking_service():
    """Test the chunking service with sample text"""
    
    # Sample legal document text
    sample_text = """
    CONTRACT FOR SALE OF REAL PROPERTY
    
    This contract establishes the terms and conditions for the sale of property located at 123 Main Street, Anytown, USA.
    
    SECTION 1: PROPERTY DESCRIPTION
    The property consists of a single-family residence with three bedrooms and two bathrooms. The property includes all fixtures and appliances currently installed.
    
    SECTION 2: PURCHASE PRICE
    The buyer agrees to pay the purchase price of $500,000 within 30 days of closing. Payment shall be made in cash or certified funds.
    
    SECTION 3: CLOSING DATE
    The closing shall occur on or before December 31, 2024, at a location to be mutually agreed upon by both parties.
    
    SECTION 4: WARRANTIES
    The seller warrants that they have good and marketable title to the property and that there are no liens or encumbrances except as disclosed in writing.
    
    This contract is binding upon both parties and their respective heirs, successors, and assigns.
    """
    
    try:
        # Initialize chunking service
        chunking_service = ChunkingService()
        
        # Test chunking
        chunks, metadata = chunking_service.chunk_text(sample_text)
        
        print("=== Chunking Results ===")
        print(f"Generated {len(chunks)} chunks")
        print(f"Original text: {metadata['original_character_count']} characters, {metadata['original_token_count']} tokens")
        print(f"Total chunk tokens: {metadata['total_tokens']}")
        print(f"Average chunk size: {metadata['average_chunk_size']:.1f} tokens")
        print(f"Chunk size range: {metadata['min_chunk_size']} - {metadata['max_chunk_size']} tokens")
        
        print("\n=== Chunks ===")
        for i, chunk in enumerate(chunks):
            print(f"\n--- Chunk {i+1} ---")
            print(chunk.strip())
            print(f"Tokens: {len(chunking_service.tokenizer.encode(chunk))}")
        
        # Test validation
        is_valid = chunking_service.validate_chunks(chunks)
        print(f"\nChunks validation: {'PASSED' if is_valid else 'FAILED'}")
        
        # Test empty text error
        print("\n=== Testing Empty Text Error ===")
        try:
            chunking_service.chunk_text("")
        except EmptyTextError as e:
            print(f"Correctly caught EmptyTextError: {e}")
        
        print("\n=== All tests completed successfully! ===")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_chunking_service() 