import os

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from fastapi.responses import JSONResponse
from src.models.rescue_predict_model import RescuePredictModel

load_dotenv()

ctx = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    session_parameters={
        "QUERY_TAG": "EndOfMonthFinancials",
    },
)


async def fetchdata():
    cursor = ctx.cursor()
    cursor.execute("USE DATABASE rescue_predict_db")
    cursor.execute("USE SCHEMA public")
    sql = 'select * from rescue_predict_db.public."accidents" order by "an" desc, "mois" desc, "jour" desc limit 100'
    df = cursor.execute(sql).fetch_pandas_all()
    records = df.to_dict(orient="records")

    return records


async def predict(input_data: RescuePredictModel):
    """
    Prediction.
    """

    response = {"prediction": 1}
    return response
