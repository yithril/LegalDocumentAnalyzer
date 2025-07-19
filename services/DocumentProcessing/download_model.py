from huggingface_hub import hf_hub_download

path = hf_hub_download(
  repo_id="facebook/bart-large-mnli",
  filename="config.json",
  repo_type="model"
)
print("config.json path:", path)

