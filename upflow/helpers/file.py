import os
from six.moves.urllib.parse import urlparse


def read_file_contents(file_name):
    with open(file_name, "r") as f:
        output = f.read()
    return output


def get_s3_uri(bucket, path, scheme="s3", host=None, port=None, aws_key=None, aws_secret_key=None):
    """

    :param bucket: name of the bucket
    :param path: path of file inside the bucket
    :param scheme: scheme for amazon s3.
                   default is s3. For proxy use s3u
    :param host: host
    :param port: port
    :param aws_key: aws key to access s3
    :param aws_secret_key: aws secret key to access s3
    :return: uri string
    """

    # s3u: // user: secret @ host:port @ mybucket / mykey.txt
    access_list = []
    if aws_key and aws_secret_key:
        access_list.append(aws_key + ":" + aws_secret_key)
    if host:
        if port:
            access_list.append(host + ":" + port)
        else:
            access_list.append(host)
    access_list.append(bucket)
    return os.path.join(scheme + "://", "@".join(access_list), path)


def remove_access_key_from_uri(uri):
    """
    returns the uri without access information
    :param uri: smart_open path
    :return: url path without access information
    """
    urlp = urlparse(uri)
    if "@" in uri:
        return (urlp.scheme + "://" if urlp.scheme else "") + uri.split("@")[-1]
    else:
        return uri
