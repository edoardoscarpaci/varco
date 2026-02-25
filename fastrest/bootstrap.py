import importlib
import pkgutil
from fastrest.registry.model_assembler import ModelAssemblerRegistry
from fastrest.models.entity import Entity

def import_submodules(package_name: str):
    package = importlib.import_module(package_name)
    for _, module_name, _ in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        importlib.import_module(module_name)


def validate_registry():
    registry = ModelAssemblerRegistry()
    missing = [
        entity.__name__
        for entity in Entity.__subclasses__()
        if not registry.exists(entity)
    ]
    if missing:
        raise RuntimeError(
            f"Missing ModelAssembler for: {', '.join(missing)}"
        )


def init_registry():
    pass