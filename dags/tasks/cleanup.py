import glob
import os
from os.path import join


def create_temp_dir(**kwargs):
    os.makedirs(f'/tmp/newsletters/{kwargs["run_id"]}', exist_ok=True)


def cleanup_files(**kwargs):
    temp_dir = f'/tmp/newsletters/{kwargs["run_id"]}'
    files = glob.glob(join(temp_dir, "*"))
    for f in files:
        os.remove(f)
    os.rmdir(temp_dir)
