from django import template
from typing import TypeVar

K = TypeVar("K")
V = TypeVar("V")

register = template.Library()


@register.filter
def dict_get(d: dict[K, V], key: K) -> V | None:
    return d.get(key)
