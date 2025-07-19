import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration for DocumentProcessing service"""
    
    # Chunking configuration
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '400'))  # Default for BART (~512 token limit)
    CHUNK_OVERLAP = int(os.getenv('CHUNK_OVERLAP', '50'))  # Overlap between chunks
    
    # Classification configuration
    CLASSIFICATION_CONFIDENCE_THRESHOLD = float(os.getenv('CLASSIFICATION_CONFIDENCE_THRESHOLD', '0.3'))
    
    # Model configuration
    LLM_MODEL_PATH = os.getenv('LLM_MODEL_PATH', 'llm-models/bart-large-mnli')
    
    # Logging configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
