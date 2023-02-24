import pydantic
from pydantic import BaseModel
from bson import ObjectId as BsonObjectId, DBRef as BsonDBRef


class ObjectId(BsonObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate
    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId.")
        return ObjectId(v)
    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")

    def __repr__(self):
        return f"{self.__class__.__name__}('{str(self)}')"


class DBRefField(BaseModel):
    collection:str
    id: ObjectId
    database: str | None = None

    class Config:
        json_encoders = {
            ObjectId: str,
        }


class DBRef(BsonDBRef):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        field = v
        if isinstance(field, dict):
            field = DBRefField(**v)

        if not isinstance(field, DBRefField):
            raise ValueError(f"Cannot construct PyDBField from value: {v}")

        return field

    @classmethod
    def __modify_schema__(cls, _):
        return DBRefField

    def __repr__(self):
        return f"{self.__class__.__name__}('{dict(self.as_doc())}')"




class DemoModel(pydantic.BaseModel):
    a: str
    b: ObjectId
    c: DBRef


    class Config:
        json_encoders = {
            DBRef: lambda d: dict(d.as_doc()),
            ObjectId: str,
        }


a = DemoModel(
    a="",
    b=ObjectId(),
    c={"collection": "batata", "id": ObjectId()},
)
print(a)
print(a.dict())
print(a.json())

print(DemoModel(
    a="",
    b=str(ObjectId()),
    c=DBRefField(collection="batata", id=str(ObjectId()))
))
