import shutil

directories_to_delete = list()


def cleanup_on_shutdown():
    for dir in directories_to_delete:
        shutil.rmtree(dir)
    directories_to_delete.clear()
