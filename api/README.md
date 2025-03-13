# Rescue Predict API

## Initialize

Copy .env.test file to .env file et set the good credentials.

Test environment :

```bash
docker build -t rescue-predict-api .
docker run -v "$(pwd):/src" -e PORT=8888 -p 8888:8888 rescue-predict-api
```

Open : http://0.0.0.0:8888/docs
