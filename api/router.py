from fastapi import APIRouter, Response

router = APIRouter(
    prefix="/",
    responses={404: {"description": "Not found"}},
)


@router.get("/test", tags=["test"])
async def test():
    """
    test route
    """
    response = {"message": "Hello from Hot Zone Road Rescue API 🎉"}
    return Response(content=response, media_type="application/json")
