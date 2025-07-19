import itertools
from pinecone import Pinecone
from config import PINECONE_API_KEY
from typing import List, Dict, Any
import asyncio

class VectorDbService:
    """Vector database service for upserting and searching document chunks."""
    
    def __init__(self, pool_threads=30):
        """Initialize the Pinecone client."""
        if not PINECONE_API_KEY:
            raise ValueError("PINECONE_API_KEY environment variable is required")
        
        self.pc = Pinecone(api_key=PINECONE_API_KEY, pool_threads=pool_threads)
    
    @staticmethod
    def _batch_chunks(chunks, batch_size=200):
        it = iter(chunks)
        batch = tuple(itertools.islice(it, batch_size))
        while batch:
            yield batch
            batch = tuple(itertools.islice(it, batch_size))

    async def upsert_chunks(self, index_host: str, chunks: List[Dict[str, Any]], namespace: str = "__default__", batch_size=200) -> bool:
        """
        Upsert document chunks to Pinecone index.
        Args:
            index_host (str): The Pinecone index host URL
            chunks (List[Dict[str, Any]]): List of chunk dicts
            namespace (str, optional): The namespace to upsert into (default: '__default__')
            batch_size (int): Batch size for upserts
        Returns:
            bool: True if successful, False otherwise
        """
        if not index_host or not index_host.strip():
            print("Error: index_host cannot be blank")
            return False
        if not namespace or not namespace.strip():
            print("Error: namespace cannot be blank")
            return False
        if not chunks:
            print("Error: chunks list cannot be empty")
            return False

        async_results = []
        errors = []
        with self.pc.Index(host=index_host) as index:
            for batch in self._batch_chunks(chunks, batch_size):
                try:
                    async_results.append(
                        index.upsert_records(namespace, list(batch), async_req=True)
                    )
                except Exception as e:
                    print(f"Error submitting upsert batch: {e}")
                    errors.append(e)
            # Wait for all upserts to complete
            loop = asyncio.get_event_loop()
            def wait_for_results():
                for i, r in enumerate(async_results):
                    try:
                        r.get()
                    except Exception as e:
                        print(f"Error in upsert batch {i}: {e}")
                        errors.append(e)
            await loop.run_in_executor(None, wait_for_results)
        if errors:
            print(f"Encountered {len(errors)} errors during upsert.")
            return False
        return True

    def semantic_search(self, index_host: str, query_text: str, top_k: int = 5, fields=None, namespace: str = "__default__"):
        """
        Perform a semantic (dense vector) search using Pinecone's integrated embedding.
        Args:
            index_host (str): The Pinecone index host URL
            query_text (str): The text to search for
            top_k (int): Number of results to return
            fields (List[str], optional): Which fields to return (default: all)
            namespace (str, optional): The namespace to search (default: '__default__')
        Returns:
            dict: The search results from Pinecone
        """
        if not index_host or not index_host.strip():
            print("Error: index_host cannot be blank")
            return None
        if not namespace or not namespace.strip():
            print("Error: namespace cannot be blank")
            return None
        if not query_text or not query_text.strip():
            print("Error: query_text cannot be blank")
            return None
        try:
            with self.pc.Index(host=index_host) as index:
                result = index.search(
                    namespace=namespace,
                    query={
                        "inputs": {"text": query_text},
                        "top_k": top_k
                    },
                    fields=fields
                )
            return result
        except Exception as e:
            print(f"Error during semantic search: {e}")
            return None
