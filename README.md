# 🚀 Jedha Lead

## Airflow Configuration 🔐

### Installation & Setup 🚀
0. Deps

```bash
# Set up env
conda create --prefix .conda python=3.13
conda activate .conda
python -m pip install pandas scikit-learn plotly poetry xgboost nbformat
# Create key
openssl req -newkey rsa:2048 -nodes -keyout private.key -out csr.csr # CN=localhost:8080
openssl x509 -req -in csr.csr -signkey private.key -out certificate.pem

```

1. Add AIRFLOW_UID env variable in .env file.

```bash
cd airflow
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

2. Download the EC2 key to airflow/ssh_keys/amazon.pem

3. Start the Airflow services:

```bash
docker compose up airflow-init
docker compose up --build
```

4. Access Airflow web interface:


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

# Deploy images on your server
docker compose up
````

#### Variables
- change airflow/variables/connections.json.base with your secrets, copy it to airflow/variables/connections.json
- check airflow/variables/variables.json

## API

### Installation & Setup 🚀

```bash
source secrets.sh
cd api
docker build -t rescue-predict-api .
chmod +x run.sh
./run.sh
```
