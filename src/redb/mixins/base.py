from pydantic import BaseModel


class Base(BaseModel):
    __database__ = None

    @classmethod
    def collection_name(cls) -> str:
        return cls.__name__.lower()

    def __repr__(self) -> str:
        class_name = self.__class__.__name__

        # add all pydantic attributes like attr=val, attr2=val2
        attributes = ", ".join(
            f"{field}={getattr(self, field)}" for field in self.__fields__
        )

        return f"{class_name}({attributes})"
