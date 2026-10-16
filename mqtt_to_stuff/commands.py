from collections.abc import Callable
from dataclasses import dataclass


def _no_op_normalize(args):
    pass


@dataclass(frozen=True)
class Command:
    name: str
    help: str
    add_arguments: Callable
    run: Callable
    normalize: Callable = _no_op_normalize
