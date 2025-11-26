import os
import shutil
from os import listdir
from os.path import isfile, join
import yaml
import json


def get_filelist(dirName):
    return [join(dirName, f) for f in listdir(dirName) if isfile(join(dirName, f))]


def delete_dir(dirName):
    shutil.rmtree(dirName)


def recreate_dir(dirName):
    if os.path.exists(dirName):
        delete_dir(dirName)
    create_dir(dirName)


def create_dir(dirName):
    if not os.path.exists(dirName):
        os.mkdir(dirName)


def is_exist(dirName):
    return os.path.exists(dirName)


def pretty_print_json(data):
    return json.dumps(data, indent=4, sort_keys=True)


def read_yaml(filename):
    data = None
    with open(filename, "r") as fp:
        try:
            data = yaml.safe_load(fp)
        except yaml.YAMLError as exc:
            print("exc: " + str(exc))

    return data
