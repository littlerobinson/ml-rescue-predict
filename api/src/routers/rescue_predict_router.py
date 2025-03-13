from fastapi import APIRouter, Response

import src.handlers.rescue_predict_handler as gh
from src.models.rescue_predict_model import RescuePredictModel

import json


router = APIRouter(
    # prefix="/",
    responses={404: {"description": "Not found"}},
)


@router.post("/predict", tags=["machine-learning"])
async def predict(data: RescuePredictModel):
    response = await gh.predict(data)
    return Response(content=json.dumps(response), media_type="application/json")


@router.get("/fetchdata", tags=["machine-learning"])
async def fetchdata():
    response = await gh.fetchdata()
    return Response(content=json.dumps(response), media_type="application/json")
