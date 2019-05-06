"""
Event Contents compression utilities

"""
import copy
import json
import zlib
import base64

from gobcore.typesystem.json import GobTypeJSONEncoder


def loads(contents):
    """
    Interpret the JSON contents string and return the corresponding data

    :param contents: JSON string
    :return: data dictionary
    """
    data = json.loads(contents)
    if data.get("_C") == "zlib":
        data = json.loads(_decompress(data["contents"]))
    return data


def dumps(data):
    """
    Converts the data into a JSON string

    :param data: data dictionary
    :return: JSON string
    """
    data_copy = copy.deepcopy(data)
    contents = json.dumps(data_copy, cls=GobTypeJSONEncoder)
    if len(contents) > 50:
        contents = json.dumps({
            "_C": "zlib",
            "contents": _compress(contents)
        }, cls=GobTypeJSONEncoder)
    return contents


def _compress(contents):
    """
    Compress any contents string
    :param contents: any contents string
    :return: base64 encoded compressed string
    """
    compress = zlib.compress(contents.encode('ascii'))
    b64encode = base64.b64encode(compress).decode('ascii')
    return b64encode


def _decompress(contents):
    """
    Decompresses a previously compressed contents string
    :param contents: base64 encoded compressed string
    :return: decompressed contents string
    """
    b64decode = base64.b64decode(contents)
    decompress = zlib.decompress(b64decode)
    return decompress
