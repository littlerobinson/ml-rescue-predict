# 🚀 Jedha Lead

## Airflow Configuration 🔐

### Installation & Setup 🚀

1. Add AIRFLOW_UID env variable in .env file.

```bash
cd airflow
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

2. Start the Airflow services:

```bash
docker compose up airflow-init
docker-compose up --build
```

4. Access Airflow web interface:

````
http://localhost:8080
Username: airflow
Password: airflow

## Requirements

- docker
- poetry

## Links to services

mlflow : [mlflow](https://huggingface.co/spaces/littlerobinson/mlflow)

## Deployment

To deploy the project :

```bash
cd mlflow

# Create local python environment
poetry install

# Build docker image
docker build . mlflow_image_name

# Deploy images on your server
````

#### Variables

Set the following variables in the Airflow Admin Interface (Admin > Variables):

```
AWS_DEFAULT_REGION    # Your AWS region (e.g., eu-west-1)
WEATHERMAP_API        # Your WeatherMap API key
S3BucketName          # Your S3 bucket name
```

### Connections

Configure the following connections in Airflow (Admin > Connections):

1. AWS Connection (`aws_id`):

   - Conn Type: Amazon Web Services
   - Configure with your AWS credentials

2. PostgreSQL Connection (`postgres_id`):
   - Conn Type: Postgres
   - Configure with your database credentials

## API

### Installation & Setup 🚀

```bash
source secrets.sh
cd api
docker build -t rescue-predict-api .
./run.sh
```
