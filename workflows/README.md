# Legal Document Processing Workflows

This directory contains the Temporal workflow orchestration for the legal document processing pipeline.

## Architecture

The workflow system consists of:

1. **DocumentProcessingWorkflow** - Main workflow that orchestrates the entire pipeline
2. **DocumentActivities** - Activities that wrap our existing services using factory pattern
3. **ServiceFactory** - Factory for dependency injection and service management
4. **DocumentProcessingWorker** - Temporal worker that runs workflows and activities
5. **State Machine** - Enforces valid state transitions (in `shared/document_state_machine.py`)

## Pipeline Steps

1. **Text Extraction** - Extract text from uploaded documents
2. **Chunking** - Split text into manageable chunks
3. **Classification** - Classify document type using BART model
4. **Vectorization** - Create embeddings and store in Pinecone
5. **Summarization** - Generate document summary

## State Machine

The workflow uses a state machine to ensure valid transitions:

- `UPLOADED` → `TEXT_EXTRACTING` → `TEXT_EXTRACTED`
- `TEXT_EXTRACTED` → `CHUNKING` → `CHUNKED`
- `CHUNKED` → `CLASSIFYING` → `CLASSIFIED`
- `CLASSIFIED` → `VECTORIZING` → `VECTORIZED`
- `VECTORIZED` → `SUMMARIZING` → `SUMMARIZED`
- `SUMMARIZED` → `COMPLETED`

Any step can transition to `FAILED`, and `FAILED` can transition back to any processing step for retry.

## Setup

### 1. Install Dependencies

```bash
cd workflows
poetry install
```

### 2. Start Temporal Server

```bash
# Using Docker
docker run --rm -p 7233:7233 temporalio/auto-setup:1.22.3

# Or install Temporal CLI and run locally
temporal server start-dev
```

### 3. Configure Environment

Create a `.env` file in the workflows directory:

```env
# Temporal Configuration
TEMPORAL_SERVER_URL=localhost:7233
TEMPORAL_TASK_QUEUE=document-processing
TEMPORAL_NAMESPACE=default

# Worker Configuration
WORKER_MAX_CONCURRENT_ACTIVITIES=10
WORKER_MAX_CONCURRENT_WORKFLOWS=5

# Timeout Configuration
ACTIVITY_START_TO_CLOSE_TIMEOUT=600
WORKFLOW_EXECUTION_TIMEOUT=3600

# Retry Configuration
MAX_RETRY_ATTEMPTS=3
INITIAL_RETRY_INTERVAL=5
MAX_RETRY_INTERVAL=300

# Logging
LOG_LEVEL=INFO
```

### 4. Start the Worker

```bash
# Using Poetry
poetry run worker

# Or directly
python workflows/worker.py
```

## Usage

### Starting a Document Processing Workflow

```python
import asyncio
from workflows.worker import DocumentProcessingWorker

async def process_document():
    # Create worker
    worker = DocumentProcessingWorker()
    
    # Start worker (in background)
    worker_task = asyncio.create_task(worker.start())
    
    # Wait for worker to be ready
    await asyncio.sleep(2)
    
    # Start document processing
    input_data = {
        "document_id": "doc-123",
        "tenant_id": "tenant-456",
        "file_path": "/path/to/document.pdf",
        "file_name": "contract.pdf",
        "mime_type": "application/pdf",
        "file_size": 1024000,
        "created_by": "user@example.com"
    }
    
    workflow_id = await worker.start_document_processing(input_data)
    print(f"Started workflow: {workflow_id}")
    
    # Keep worker running
    await worker_task

# Run
asyncio.run(process_document())
```

### Monitoring Workflows

You can monitor workflows using the Temporal Web UI:

1. Start Temporal Web UI: `temporal web start`
2. Open browser to `http://localhost:8080`
3. Navigate to Workflows to see running workflows

## Dependency Injection

The system uses a factory pattern for dependency injection:

1. **ServiceFactory** - Manages service instances and lifecycle
2. **Singleton Services** - Stateless services (text extraction, chunking, etc.) are singletons
3. **Per-Tenant Services** - DocumentService is created per tenant for database isolation
4. **Lazy Loading** - Services are created on first use
5. **Service Caching** - DocumentService instances are cached by tenant ID

## Error Handling

The workflow includes comprehensive error handling:

1. **Step Failures** - Each step can fail and be retried
2. **State Validation** - Invalid state transitions are prevented
3. **Detailed Error Logging** - Errors are logged with context
4. **Retry Policies** - Configurable retry with exponential backoff
5. **Failure Recovery** - Failed documents can be restarted from any step

## Configuration

All configuration is handled through environment variables and the `WorkflowConfig` class:

- **Timeouts** - Activity and workflow execution timeouts
- **Retry Policies** - Retry attempts and intervals
- **Concurrency** - Maximum concurrent activities and workflows
- **Logging** - Log levels and formats

## Testing

```bash
# Run tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=workflows
```

## Deployment

The worker can be deployed as a service:

1. **Docker** - Build container with all dependencies
2. **Kubernetes** - Deploy as a deployment with multiple replicas
3. **Systemd** - Run as a system service
4. **Cloud Functions** - Deploy as serverless functions

## Monitoring and Observability

- **Temporal Web UI** - Monitor workflows and activities
- **Logging** - Structured logging with correlation IDs
- **Metrics** - Temporal provides built-in metrics
- **Tracing** - Distributed tracing across workflow steps 