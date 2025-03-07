from pydantic import BaseModel
from typing import Union


class RescuePredictModel(BaseModel):
    var1: Union[int, float]
    var2: Union[int, float]
    var3: bool
