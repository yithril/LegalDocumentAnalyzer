from langchain.text_splitter import RecursiveCharacterTextSplitter
from transformers import AutoTokenizer
from typing import List, Dict, Any, Tuple
import logging
from config import Config

logger = logging.getLogger(__name__)

class EmptyTextError(Exception):
    """Raised when text is empty or contains no meaningful content"""
    pass

class ChunkingError(Exception):
    """Raised when chunking fails"""
    pass

class ChunkingService:
    """Service for chunking text using LangChain with configurable token limits"""
    
    def __init__(self, model_path: str = None, chunk_size: int = None, chunk_overlap: int = None):
        """
        Initialize chunking service
        
        Args:
            model_path: Path to the tokenizer model (defaults to config)
            chunk_size: Maximum tokens per chunk (defaults to config)
            chunk_overlap: Token overlap between chunks (defaults to config)
        """
        self.model_path = model_path or Config.LLM_MODEL_PATH
        self.chunk_size = chunk_size or Config.CHUNK_SIZE
        self.chunk_overlap = chunk_overlap or Config.CHUNK_OVERLAP
        
        # Initialize tokenizer for precise token counting
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_path)
            logger.info(f"Initialized tokenizer from {self.model_path}")
        except Exception as e:
            logger.error(f"Failed to initialize tokenizer: {str(e)}")
            raise ChunkingError(f"Tokenizer initialization failed: {str(e)}")
        
        # Initialize text splitter
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            length_function=self._token_count,
            separators=["\n\n", "\n", ". ", " ", ""]  # Try to split at paragraph breaks first, then sentences, then words
        )
        
        logger.info(f"Initialized chunking service with chunk_size={self.chunk_size}, overlap={self.chunk_overlap}")
    
    def _token_count(self, text: str) -> int:
        """
        Count tokens in text using the tokenizer
        
        Args:
            text: Text to count tokens for
            
        Returns:
            Number of tokens
        """
        try:
            return len(self.tokenizer.encode(text))
        except Exception as e:
            logger.error(f"Token counting failed: {str(e)}")
            # Fallback to character count as rough approximation
            return len(text)
    
    def chunk_text(self, text: str) -> Tuple[List[str], Dict[str, Any]]:
        """
        Chunk text into smaller pieces
        
        Args:
            text: Text to chunk
            
        Returns:
            Tuple of (chunks, metadata)
            
        Raises:
            EmptyTextError: If text is empty or contains no meaningful content
            ChunkingError: If chunking fails
        """
        logger.info(f"Starting text chunking. Input text length: {len(text)} characters")
        
        # Validate input
        if not text or not text.strip():
            logger.error("Text is empty or contains only whitespace")
            raise EmptyTextError("Text is empty or contains no meaningful content")
        
        # Check if text is too short to chunk meaningfully
        token_count = self._token_count(text)
        if token_count <= self.chunk_size:
            logger.info(f"Text is shorter than chunk size ({token_count} tokens), returning as single chunk")
            chunks = [text]
        else:
            try:
                # Perform chunking
                chunks = self.text_splitter.split_text(text)
                logger.info(f"Successfully chunked text into {len(chunks)} chunks")
                
                # Validate chunks
                if not chunks:
                    raise EmptyTextError("Chunking produced no chunks")
                
                # Check for empty chunks
                non_empty_chunks = [chunk for chunk in chunks if chunk.strip()]
                if not non_empty_chunks:
                    raise EmptyTextError("All chunks are empty after chunking")
                
                chunks = non_empty_chunks
                
            except Exception as e:
                logger.error(f"Chunking failed: {str(e)}")
                raise ChunkingError(f"Text chunking failed: {str(e)}")
        
        # Generate metadata
        metadata = self._generate_metadata(text, chunks)
        
        logger.info(f"Chunking completed. Generated {len(chunks)} chunks with {metadata['total_tokens']} total tokens")
        return chunks, metadata
    
    def _generate_metadata(self, original_text: str, chunks: List[str]) -> Dict[str, Any]:
        """
        Generate metadata about the chunking process
        
        Args:
            original_text: Original text that was chunked
            chunks: List of chunks
            
        Returns:
            Dictionary containing chunking metadata
        """
        original_tokens = self._token_count(original_text)
        chunk_tokens = [self._token_count(chunk) for chunk in chunks]
        total_chunk_tokens = sum(chunk_tokens)
        
        return {
            "original_character_count": len(original_text),
            "original_token_count": original_tokens,
            "chunk_count": len(chunks),
            "chunk_sizes": chunk_tokens,
            "total_tokens": total_chunk_tokens,
            "average_chunk_size": sum(chunk_tokens) / len(chunks) if chunks else 0,
            "max_chunk_size": max(chunk_tokens) if chunks else 0,
            "min_chunk_size": min(chunk_tokens) if chunks else 0,
            "chunking_method": "recursive_character_splitter",
            "chunk_size_limit": self.chunk_size,
            "chunk_overlap": self.chunk_overlap,
            "chunking_successful": True
        }
    
    def validate_chunks(self, chunks: List[str]) -> bool:
        """
        Validate that chunks are reasonable
        
        Args:
            chunks: List of chunks to validate
            
        Returns:
            True if chunks are valid, False otherwise
        """
        if not chunks:
            logger.warning("No chunks to validate")
            return False
        
        for i, chunk in enumerate(chunks):
            if not chunk.strip():
                logger.warning(f"Chunk {i} is empty or contains only whitespace")
                return False
            
            token_count = self._token_count(chunk)
            if token_count > self.chunk_size:
                logger.warning(f"Chunk {i} exceeds token limit: {token_count} > {self.chunk_size}")
                return False
        
        logger.debug(f"Validated {len(chunks)} chunks successfully")
        return True 