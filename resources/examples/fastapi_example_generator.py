from enum import Enum
from typing import TypeVar

import uvicorn
from fastapi import Body, FastAPI
from pydantic import BaseModel

from redb.core.utils import generate_examples

app = FastAPI()


class Animal(BaseModel):
    name: str
    age: int


class DogBreeds(str, Enum):
    TELOMIAN = "telomian"
    OTTHERHOUND = "otterhound"
    GREAT_DANE = "great dane"
    GOLDEN = "golden"


class CatBreeds(str, Enum):
    PERSIAN = "persian"
    SIAMESE = "siamese"
    BRITISH_SHORTHAIR = "british shorthair"
    MAINE_COON = "maine coon"


class CreateDog(Animal):
    weight: int
    size: int = 10
    breed: DogBreeds = DogBreeds.GOLDEN


class CreateCat(Animal):
    claws_size: int
    color_number: int = 2
    breed: CatBreeds


examples = generate_examples([CreateDog, CreateCat])
T = TypeVar("T", CreateDog, CreateCat)


@app.post("/with-examples", status_code=201)
def with_examples(input: T = Body(..., examples=examples)) -> T:
    return input


@app.post("/without-examples", status_code=201)
def without_examples(input: T = Body(...)) -> T:
    return input


if __name__ == "__main__":
    uvicorn.run(app=app)
