import shutil
import sys

directories_to_delete = []


def cleanup_on_shutdown(signum=None, _frame=None):
    for directory in directories_to_delete:
        shutil.rmtree(directory)
    directories_to_delete.clear()
    if signum is not None:
        sys.exit(signum)
