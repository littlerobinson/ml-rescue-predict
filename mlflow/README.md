# 🚀 MLFLOW

## Requirements

- docker
- poetry

## Links to services

mlflow : [mlflow](https://huggingface.co/spaces/littlerobinson/mlflow)

## Clone the project

```bash
git clone git@hf.co:spaces/littlerobinson/mlflow
```

## Deployment

To deploy the project :

```bash
cd mlflow

# Create local python environment
poetry install

# Build docker image
docker build . mlflow_image_name

# Deploy images on your server
```
