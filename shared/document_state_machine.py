from enum import Enum
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

class DocumentState(Enum):
    """Document processing states"""
    
    # Initial state
    UPLOADED = "uploaded"
    
    # Processing states
    TEXT_EXTRACTING = "text_extracting"
    TEXT_EXTRACTED = "text_extracted"
    CHUNKING = "chunking"
    CHUNKED = "chunked"
    CLASSIFYING = "classifying"
    CLASSIFIED = "classified"
    VECTORIZING = "vectorizing"
    VECTORIZED = "vectorized"
    SUMMARIZING = "summarizing"
    SUMMARIZED = "summarized"
    
    # Final states
    COMPLETED = "completed"
    FAILED = "failed"

class DocumentStateMachine:
    """State machine for document processing"""
    
    # Define allowed transitions
    ALLOWED_TRANSITIONS = {
        DocumentState.UPLOADED: [DocumentState.TEXT_EXTRACTING, DocumentState.FAILED],
        
        DocumentState.TEXT_EXTRACTING: [DocumentState.TEXT_EXTRACTED, DocumentState.FAILED],
        DocumentState.TEXT_EXTRACTED: [DocumentState.CHUNKING, DocumentState.FAILED],
        
        DocumentState.CHUNKING: [DocumentState.CHUNKED, DocumentState.FAILED],
        DocumentState.CHUNKED: [DocumentState.CLASSIFYING, DocumentState.FAILED],
        
        DocumentState.CLASSIFYING: [DocumentState.CLASSIFIED, DocumentState.FAILED],
        DocumentState.CLASSIFIED: [DocumentState.VECTORIZING, DocumentState.FAILED],
        
        DocumentState.VECTORIZING: [DocumentState.VECTORIZED, DocumentState.FAILED],
        DocumentState.VECTORIZED: [DocumentState.SUMMARIZING, DocumentState.FAILED],
        
        DocumentState.SUMMARIZING: [DocumentState.SUMMARIZED, DocumentState.FAILED],
        DocumentState.SUMMARIZED: [DocumentState.COMPLETED],
        
        # Failed state can transition back to any processing state for retry
        DocumentState.FAILED: [
            DocumentState.TEXT_EXTRACTING,
            DocumentState.CHUNKING,
            DocumentState.CLASSIFYING,
            DocumentState.VECTORIZING,
            DocumentState.SUMMARIZING
        ],
        
        # Terminal states
        DocumentState.COMPLETED: []
    }
    
    @classmethod
    def can_transition(cls, from_state: DocumentState, to_state: DocumentState) -> bool:
        """
        Check if transition from from_state to to_state is allowed
        
        Args:
            from_state: Current document state
            to_state: Target document state
            
        Returns:
            True if transition is allowed, False otherwise
        """
        allowed_transitions = cls.ALLOWED_TRANSITIONS.get(from_state, [])
        return to_state in allowed_transitions
    
    @classmethod
    def get_allowed_transitions(cls, current_state: DocumentState) -> List[DocumentState]:
        """
        Get list of allowed transitions from current state
        
        Args:
            current_state: Current document state
            
        Returns:
            List of allowed target states
        """
        return cls.ALLOWED_TRANSITIONS.get(current_state, [])
    
    @classmethod
    def validate_transition(cls, from_state: DocumentState, to_state: DocumentState) -> None:
        """
        Validate transition and raise exception if invalid
        
        Args:
            from_state: Current document state
            to_state: Target document state
            
        Raises:
            InvalidStateTransitionError: If transition is not allowed
        """
        if not cls.can_transition(from_state, to_state):
            allowed = cls.get_allowed_transitions(from_state)
            raise InvalidStateTransitionError(
                f"Cannot transition from {from_state.value} to {to_state.value}. "
                f"Allowed transitions: {[s.value for s in allowed]}"
            )
    
    @classmethod
    def get_processing_step_for_state(cls, state: DocumentState) -> Optional[str]:
        """
        Get the processing step name for a given state
        
        Args:
            state: Document state
            
        Returns:
            Processing step name or None if not a processing state
        """
        processing_states = {
            DocumentState.TEXT_EXTRACTING: "text_extraction",
            DocumentState.CHUNKING: "chunking",
            DocumentState.CLASSIFYING: "classification",
            DocumentState.VECTORIZING: "vectorization",
            DocumentState.SUMMARIZING: "summarization"
        }
        return processing_states.get(state)
    
    @classmethod
    def is_processing_state(cls, state: DocumentState) -> bool:
        """
        Check if state is a processing state (actively working)
        
        Args:
            state: Document state
            
        Returns:
            True if state is a processing state
        """
        processing_states = {
            DocumentState.TEXT_EXTRACTING,
            DocumentState.CHUNKING,
            DocumentState.CLASSIFYING,
            DocumentState.VECTORIZING,
            DocumentState.SUMMARIZING
        }
        return state in processing_states
    
    @classmethod
    def is_terminal_state(cls, state: DocumentState) -> bool:
        """
        Check if state is a terminal state (no further transitions)
        
        Args:
            state: Document state
            
        Returns:
            True if state is terminal
        """
        terminal_states = {DocumentState.COMPLETED, DocumentState.FAILED}
        return state in terminal_states
    
    @classmethod
    def get_retry_state(cls, failed_state: DocumentState) -> Optional[DocumentState]:
        """
        Get the appropriate retry state when transitioning from failed
        
        Args:
            failed_state: The state that failed (stored in error_details)
            
        Returns:
            State to retry from, or None if no retry possible
        """
        # Map failed states to their retry states
        retry_mapping = {
            "text_extraction": DocumentState.TEXT_EXTRACTING,
            "chunking": DocumentState.CHUNKING,
            "classification": DocumentState.CLASSIFYING,
            "vectorization": DocumentState.VECTORIZING,
            "summarization": DocumentState.SUMMARIZING
        }
        
        if failed_state == DocumentState.FAILED:
            # This would be determined by the error_details.step field
            # For now, return None - the workflow will need to determine this
            return None
        
        return retry_mapping.get(failed_state.value)

class InvalidStateTransitionError(Exception):
    """Raised when an invalid state transition is attempted"""
    pass 