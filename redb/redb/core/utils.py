from enum import Enum
from typing import Any, Type, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


DEFAULT_VALUES = {"str": "string", "float": 0.0, "int": 0, "dict": {}, "list": []}


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
            if field_value is None:
                field_value = field.default_factory()

            if isinstance(field_value, Enum):
                field_value = field_value.value
            elif isinstance(field_value, BaseModel):
                field_value = _get_value(field_value.__class__)

            value[name] = field_value
            continue

        if issubclass(field.type_, BaseModel):
            value[name] = _get_value(field.type_)
        elif issubclass(field.type_, Enum):
            value[name] = next(iter(field.type_)).value
        elif field.outer_type_.__name__ in DEFAULT_VALUES:
            value[name] = DEFAULT_VALUES[field.outer_type_.__name__]
        elif field.type_.__name__ in DEFAULT_VALUES:
            value[name] = DEFAULT_VALUES[field.type_.__name__]
        else:
            value[name] = field.type_.__name__

    return value
