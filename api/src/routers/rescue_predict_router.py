from fastapi import APIRouter, Response

import api.src.handlers.rescue_predict_handler as gh
from src.models.rescue_predict_model import RescuePredictModel

import json


router = APIRouter(
    prefix="/",
    # tags=["getaround"],
    responses={404: {"description": "Not found"}},
)


@router.post("/predict", tags=["machine-learning"])
async def predict(data: RescuePredictModel):
    response = await gh.predict(data)
    return Response(content=json.dumps(response), media_type="application/json")
