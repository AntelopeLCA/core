import os
import re
import gzip as gz
import json


def from_json(fname):
    """
    Routine to extract the contents of a json file.  no longer pretends to support PY2.
    :param fname: json file, optionally gzipped
    :return: a json-derived dict
    """
    print('Loading JSON data from %s:' % fname)
    if bool(re.search('\.gz$', fname)):
        with gz.open(fname, 'rt') as fp:
            j = json.load(fp)
    else:
        with open(fname, 'r') as fp:
            j = json.load(fp)
    return j


def to_json(obj, fname, gzip=False):
    dirname = os.path.dirname(fname)
    if not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)
    if gzip is True:
        if not bool(re.search('\.gz$', fname)):
            fname += '.gz'
        try:  # python3
            with gz.open(fname, 'wt') as fp:
                json.dump(obj, fp, indent=2, sort_keys=True)
        except ValueError:  # python2
            with gz.open(fname, 'w') as fp:
                json.dump(obj, fp, indent=2, sort_keys=True)
    else:
        with open(fname, 'w') as fp:
            json.dump(obj, fp, indent=2, sort_keys=True)
