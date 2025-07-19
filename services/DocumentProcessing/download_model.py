import os
from huggingface_hub import snapshot_download

# Create a models directory if it doesn't exist
models_dir = "llm-models"
if not os.path.exists(models_dir):
    os.makedirs(models_dir)

# Download the complete model to a local directory
model_path = snapshot_download(
    repo_id="facebook/bart-large-mnli",
    local_dir=os.path.join(models_dir, "bart-large-mnli"),
    local_dir_use_symlinks=False  # This ensures actual files are downloaded, not symlinks
)

print(f"Model downloaded to: {model_path}")
print("You can now use this local path in your code instead of downloading each time.")

