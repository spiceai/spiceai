# This isn't 100% safe, but is much safer than just blindly evaluating code.


def somewhat_safe_exec(code: str, locals_dict: dict) -> dict:
    exec(code, {"__builtins__": {}}, locals_dict)  # pylint: disable=exec-used
    return locals_dict


def somewhat_safe_eval(code: str, locals_dict: dict) -> any:
    return eval(code, {"__builtins__": {}}, locals_dict)  # pylint: disable=eval-used
