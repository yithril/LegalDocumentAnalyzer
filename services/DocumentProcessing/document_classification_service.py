from transformers import AutoTokenizer, AutoModelForSequenceClassification
from typing import List, Dict, Any, Tuple
import torch
import logging
from config import Config

logger = logging.getLogger(__name__)

class ClassificationError(Exception):
    """Raised when document classification fails"""
    pass

class DocumentClassificationService:
    """Service for classifying document types using BART model"""
    
    def __init__(self, model_path: str = None):
        """
        Initialize classification service
        
        Args:
            model_path: Path to the BART model (defaults to config)
        """
        self.model_path = model_path or Config.LLM_MODEL_PATH
        
        # Initialize model and tokenizer
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_path)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_path)
            logger.info(f"Initialized classification model from {self.model_path}")
        except Exception as e:
            logger.error(f"Failed to initialize classification model: {str(e)}")
            raise ClassificationError(f"Model initialization failed: {str(e)}")
        
        # Define document type candidates
        self.document_types = [
            "contract",
            "legal brief",
            "court filing",
            "policy document",
            "legal memo",
            "agreement",
            "letter",
            "notice",
            "affidavit",
            "deposition",
            "exhibit",
            "pleading",
            "motion",
            "order",
            "judgment",
            "email"
        ]
        
        # Add "unknown" as a valid classification for failed classifications
        self.unknown_type = "unknown"
        
        logger.info(f"Initialized classification service with {len(self.document_types)} document types")
    
    def classify_document(self, chunks: List[str]) -> Tuple[str, Dict[str, Any]]:
        """
        Classify document type using majority voting from chunks
        
        Args:
            chunks: List of text chunks to classify
            
        Returns:
            Tuple of (document_type, metadata)
            
        Raises:
            ClassificationError: If classification fails
        """
        logger.info(f"Starting document classification with {len(chunks)} chunks")
        
        if not chunks:
            logger.error("No chunks provided for classification")
            raise ClassificationError("No chunks provided for classification")
        
        try:
            # Classify each chunk
            chunk_classifications = []
            chunk_scores = []
            
            for i, chunk in enumerate(chunks):
                logger.debug(f"Classifying chunk {i+1}/{len(chunks)}")
                chunk_type, chunk_score = self._classify_chunk(chunk)
                chunk_classifications.append(chunk_type)
                chunk_scores.append(chunk_score)
            
            # Determine final classification using majority voting
            final_type, confidence = self._majority_vote(chunk_classifications, chunk_scores)
            
            # Generate metadata
            metadata = self._generate_metadata(chunks, chunk_classifications, chunk_scores, final_type, confidence)
            
            logger.info(f"Document classified as '{final_type}' with confidence {confidence:.2f}")
            return final_type, metadata
            
        except Exception as e:
            logger.error(f"Document classification failed: {str(e)}")
            raise ClassificationError(f"Classification failed: {str(e)}")
    
    def _classify_chunk(self, chunk: str) -> Tuple[str, float]:
        """
        Classify a single chunk using BART model
        
        Args:
            chunk: Text chunk to classify
            
        Returns:
            Tuple of (document_type, confidence_score)
        """
        try:
            # Prepare inputs for the model
            inputs = self.tokenizer(
                chunk,
                return_tensors="pt",
                truncation=True,
                max_length=512,
                padding=True
            )
            
            # Get model predictions
            with torch.no_grad():
                outputs = self.model(**inputs)
                logits = outputs.logits
                probabilities = torch.softmax(logits, dim=1)
            
            # Get the predicted class and confidence
            predicted_class = torch.argmax(probabilities, dim=1).item()
            confidence = probabilities[0][predicted_class].item()
            
            # Map to document type (simplified for MVP)
            # In a real implementation, you'd have a proper mapping
            document_type = self.document_types[predicted_class % len(self.document_types)]
            
            return document_type, confidence
            
        except Exception as e:
            logger.error(f"Chunk classification failed: {str(e)}")
            # Return unknown classification with low confidence for this chunk
            return self.unknown_type, 0.0
    
    def _majority_vote(self, classifications: List[str], scores: List[float]) -> Tuple[str, float]:
        """
        Determine final classification using majority voting with confidence weighting
        
        Args:
            classifications: List of chunk classifications
            scores: List of confidence scores
            
        Returns:
            Tuple of (final_type, overall_confidence)
        """
        # Count votes for each type
        vote_counts = {}
        weighted_scores = {}
        
        for doc_type, score in zip(classifications, scores):
            if doc_type not in vote_counts:
                vote_counts[doc_type] = 0
                weighted_scores[doc_type] = 0.0
            
            vote_counts[doc_type] += 1
            weighted_scores[doc_type] += score
        
        # Find the type with the most votes
        if not vote_counts:
            return self.unknown_type, 0.0
        
        max_votes = max(vote_counts.values())
        most_voted_types = [doc_type for doc_type, votes in vote_counts.items() if votes == max_votes]
        
        # If tie, choose the one with highest weighted score
        if len(most_voted_types) == 1:
            final_type = most_voted_types[0]
        else:
            # Break tie by weighted score
            final_type = max(most_voted_types, key=lambda x: weighted_scores[x])
        
        # Calculate overall confidence
        total_votes = len(classifications)
        vote_percentage = vote_counts[final_type] / total_votes
        avg_confidence = weighted_scores[final_type] / vote_counts[final_type]
        overall_confidence = (vote_percentage + avg_confidence) / 2
        
        # If confidence is too low, mark as unknown
        if overall_confidence < Config.CLASSIFICATION_CONFIDENCE_THRESHOLD:
            logger.warning(f"Low confidence classification ({overall_confidence:.2f}), marking as unknown")
            return self.unknown_type, overall_confidence
        
        return final_type, overall_confidence
    
    def _generate_metadata(self, chunks: List[str], classifications: List[str], 
                          scores: List[float], final_type: str, confidence: float) -> Dict[str, Any]:
        """
        Generate metadata about the classification process
        
        Args:
            chunks: Original chunks
            classifications: Individual chunk classifications
            scores: Individual chunk confidence scores
            final_type: Final document type
            confidence: Overall confidence
            
        Returns:
            Dictionary containing classification metadata
        """
        # Count votes for each type
        vote_counts = {}
        for doc_type in classifications:
            vote_counts[doc_type] = vote_counts.get(doc_type, 0) + 1
        
        return {
            "final_document_type": final_type,
            "overall_confidence": confidence,
            "chunk_count": len(chunks),
            "chunk_classifications": classifications,
            "chunk_confidence_scores": scores,
            "vote_distribution": vote_counts,
            "classification_method": "majority_vote_with_confidence",
            "model_used": self.model_path,
            "available_document_types": self.document_types,
            "classification_successful": True
        }
    
    def get_available_document_types(self) -> List[str]:
        """Get list of available document types for classification"""
        return self.document_types.copy() 