import sys
import os
import re
import json
from poyonga import Groonga

IGNORE_DIRS = ('.git', '.hg')
IGNORE_EXTS = ('.pyc', '.egg', '.doctree', '.pickle')
TARGET_EXTS = ('.py', '.pl', '.c', '.h')


def _load(path):
    grn = Groonga()
    filename = os.path.basename(path)
    ext = os.path.splitext(filename)[1]
    content = open(path).read()
    data = """[{"_key":"%s","name":"%s","content":"%s","type":"%s"}]""" % (
        path, filename, re.escape(content), ext)
    data = [{"_key": path, "name": filename, "content": content, "ext": ext}]
    data = json.dumps(data)
    ret = grn.call("load", table="Files", values=data)
    print(ret.body)


def execute(path):
    for dir_or_file in os.listdir(path):
        filename = os.path.join(path, dir_or_file)
        if os.path.isfile(filename):
            ext = os.path.splitext(filename)[1]
            if ext in IGNORE_EXTS:
                continue
            if ext not in TARGET_EXTS:
                continue
            print(filename)
            _load(filename)
        if os.path.isdir(filename):
            if dir_or_file in IGNORE_DIRS:
                continue
            execute(filename)


if __name__ == '__main__':
    execute(sys.argv[1])
