import types
from enum import Enum
from typing import Any, Type, TypeVar

from pydantic import BaseModel
from pydantic.fields import ModelField

T = TypeVar("T", bound=BaseModel)

DEFAULT_CONTAINER_VALUES = {"list": [], "tuple": (), "set": set()}
DEFAULT_PRIMITIVE_VALUES = {
    "str": "string",
    "float": 0.0,
    "int": 0,
    "bool": False,
    "None": None,
    "bytes": b"",
}
DEFAULT_VALUES = DEFAULT_PRIMITIVE_VALUES | DEFAULT_CONTAINER_VALUES


def generate_examples(pydantic_models: list[Type[T]]) -> dict[str, dict[str, Any]]:
    out = {}
    for model in pydantic_models:
        example = get_example_from_pydantic(model)
        out[example["summary"]] = example

    return out


def get_example_from_pydantic(pydantic_model: Type[T]) -> dict[str, Any]:
    class_name = pydantic_model.__name__
    class_doc = pydantic_model.__doc__

    summary = _get_summary(class_name)
    description = class_doc.strip() if class_doc is not None else ""
    value = _get_value(pydantic_model)

    return {"summary": summary, "description": description, "value": value}


def _get_summary(class_name: str) -> str:
    summary = class_name[0]
    for char in class_name[1:]:
        if char.isupper():
            summary += " "
        summary += char

    return summary


def _get_value(pydantic_model: Type[T]) -> dict:
    value = {}
    for name, field in pydantic_model.__fields__.items():
        if field.default is not None or field.default_factory is not None:
            field_value = field.default
            if field_value is None and field.default_factory is not None:
                field_value = field.default_factory()

            if isinstance(field_value, Enum):
                field_value = field_value.value
            elif isinstance(field_value, BaseModel):
                field_value = _get_value(field_value.__class__)

            value[name] = field_value
            continue

        value[name] = _parse_value(field)

    return value


def _parse_value(field: ModelField):
    value = None
    if isinstance(field.type_, types.UnionType):
        field.type_ = field.type_.__args__[0]

    if isinstance(field.outer_type_, types.UnionType):
        field.outer_type_ = field.outer_type_.__args__[0]

    if field.type_.__name__ in DEFAULT_CONTAINER_VALUES:
        try:
            tmp = field.type_
            field.type_ = field.type_.__args__[0]
            field.outer_type_ = tmp
        except:
            return DEFAULT_CONTAINER_VALUES[field.type_.__name__]

    if field.type_.__name__ in DEFAULT_PRIMITIVE_VALUES:
        value = DEFAULT_PRIMITIVE_VALUES[field.type_.__name__]
    elif issubclass(field.type_, BaseModel):
        value = _get_value(field.type_)
    elif issubclass(field.type_, Enum):
        value = next(iter(field.type_)).value
    else:
        value = field.type_.__name__

    if field.outer_type_.__name__ in DEFAULT_CONTAINER_VALUES:
        value = _build_container(value, field)
    
    return value
        


def _build_container(value: Any, field: ModelField):
    if field.outer_type_.__name__ == "list":
        return [value] if value is not None else []
    elif field.outer_type_.__name__ == "set":
        return {value,} if value is not None else set()
    elif field.outer_type_.__name__ == "tuple":
        return (value,) if value is not None else tuple()
    return value