import logging
import sys
import os

# Add the parent directory to the path so we can import the service
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from document_classification_service import DocumentClassificationService, ClassificationError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_classification_service():
    """Test the document classification service with sample chunks"""
    
    # Sample chunks from different document types
    contract_chunks = [
        "This contract establishes the terms and conditions for the sale of property located at 123 Main Street.",
        "The buyer agrees to pay the purchase price of $500,000 within 30 days of closing.",
        "The seller warrants that they have good and marketable title to the property."
    ]
    
    brief_chunks = [
        "MEMORANDUM OF LAW IN SUPPORT OF MOTION TO DISMISS",
        "The defendant moves this Court to dismiss the complaint for failure to state a claim upon which relief can be granted.",
        "The plaintiff has failed to allege sufficient facts to support their cause of action."
    ]
    
    try:
        # Initialize classification service
        classification_service = DocumentClassificationService()
        
        print("=== Available Document Types ===")
        doc_types = classification_service.get_available_document_types()
        print(f"Available types: {', '.join(doc_types)}")
        
        # Test contract classification
        print("\n=== Testing Contract Classification ===")
        contract_type, contract_metadata = classification_service.classify_document(contract_chunks)
        print(f"Classified as: {contract_type}")
        print(f"Confidence: {contract_metadata['overall_confidence']:.2f}")
        print(f"Vote distribution: {contract_metadata['vote_distribution']}")
        
        # Test brief classification
        print("\n=== Testing Brief Classification ===")
        brief_type, brief_metadata = classification_service.classify_document(brief_chunks)
        print(f"Classified as: {brief_type}")
        print(f"Confidence: {brief_metadata['overall_confidence']:.2f}")
        print(f"Vote distribution: {brief_metadata['vote_distribution']}")
        
        # Test empty chunks error
        print("\n=== Testing Empty Chunks Error ===")
        try:
            classification_service.classify_document([])
        except ClassificationError as e:
            print(f"Correctly caught ClassificationError: {e}")
        
        print("\n=== All tests completed successfully! ===")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_classification_service() 