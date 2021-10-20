import shutil
import sys

directories_to_delete = list()


def cleanup_on_shutdown(signum=None, _frame=None):
    for dir in directories_to_delete:
        shutil.rmtree(dir)
    directories_to_delete.clear()
    if signum is not None:
        sys.exit(signum)
