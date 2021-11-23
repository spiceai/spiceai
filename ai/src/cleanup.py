import shutil

directories_to_delete = []


def cleanup_on_shutdown():
    for directory in directories_to_delete:
        shutil.rmtree(directory)
    directories_to_delete.clear()
