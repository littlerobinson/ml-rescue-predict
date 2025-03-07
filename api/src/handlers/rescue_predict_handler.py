import os
import mlflow
import pandas as pd

from src.models.rescue_predict_model import RescuePredictModel


async def predict(input_data: RescuePredictModel):
    """
    Prediction.

    Args:
        {
            date: string
            jour_ferie: bool
            vacances_Zone_A: bool
            vacances_Zone_B: bool
            vacances_Zone_C: bool
        }
    """

    # Transform data
    df = pd.DataFrame(dict(input_data), index=[0])

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["weekday"] = df["date"].dt.weekday  # Lundi = 0, Dimanche = 6
    df["weekend"] = df["weekday"].apply(
        lambda x: 1 if x >= 5 else 0
    )  # 1 si samedi/dimanche
    df.drop(columns=["date"], inplace=True)
    bool_cols = [
        "jour_ferie",
        "vacances_Zone_A",
        "vacances_Zone_B",
        "vacances_Zone_C",
    ]
    df[bool_cols] = df[bool_cols].astype(int)

    MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    # Log model from mlflow
    MLFLOW_LOGGED_MODEL = os.getenv("MLFLOW_LOGGED_MODEL")
    logged_model = MLFLOW_LOGGED_MODEL

    # Load model as a PyFuncModel.
    loaded_model = mlflow.pyfunc.load_model(logged_model)

    prediction = loaded_model.predict(df)

    # Format response
    response = {"prediction": prediction.tolist()[0]}
    return response
