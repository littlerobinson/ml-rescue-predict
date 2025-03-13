import snowflake.connector
from fastapi.responses import JSONResponse
import pandas as pd

from src.models.rescue_predict_model import RescuePredictModel

ctx = snowflake.connector.connect(
    user="alex",
    password="Halt5-Dumpling2-Radar4-Pasted3-Volumes9",
    account="UGPSTAX-HM40418",
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
