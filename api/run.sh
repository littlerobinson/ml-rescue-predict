docker run -it \
-v "$(pwd):/src" \
-e MLFLOW_TRACKING_URI="https://littlerobinson-mlflow.hf.space" \
" \
-p 4000:80 \
rescue-predict-api