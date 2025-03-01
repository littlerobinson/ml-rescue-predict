from fastapi import FastAPI

import router

tags_metadata = [
    {"name": "data", "description": "Show data"},
    {"name": "machine-learning", "description": "Prediction Endpoint."},
]

app = FastAPI(
    title="🪐 Hot Zone Road Rescue API",
    description="API for Hot Zone Road Rescue",
    version="0.1",
    contact={
        "url": "https://github.com/littlerobinson",
    },
    openapi_tags=tags_metadata,
)

app.include_router(router.router)
