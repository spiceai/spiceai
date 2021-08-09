def print_event(
    pod_name: str,
    message: str,
):
    print(f"{pod_name} -> {message}", flush=True)
