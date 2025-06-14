# Training service

## Test training

Set your .env file.

```bash
docker build -t ml-rescue-predict-training .

chmod +x run.sh

./run.sh
```

## Troubleshooting with snowflake

Desactivate MFA dor 30 minutes.

```sql
ALTER USER joe SET MINS_TO_BYPASS_MFA = 30;
```
