import importlib.util

# This isn't 100% safe, but is much safer than just blindly evaluating code.


def somewhat_safe_exec(code: str, locals_dict: dict) -> dict:
    exec(code, {"__builtins__": {}}, locals_dict)  # pylint: disable=exec-used
    return locals_dict


def somewhat_safe_eval(code: str, locals_dict: dict) -> any:
    return eval(code, {"__builtins__": {}}, locals_dict)  # pylint: disable=eval-used


def load_module_from_code(code: str, module_name: str):
    spec = importlib.util.spec_from_loader(module_name, loader=None)
    module = importlib.util.module_from_spec(spec)

    exec(code, module.__dict__)  # pylint: disable=exec-used
    return module
