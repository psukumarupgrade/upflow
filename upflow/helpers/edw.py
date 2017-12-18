from upflow.helpers.file import get_s3_uri
from airflow.models import Variable
import json


class EDW(object):
    @staticmethod
    def get_conn_id():
        return 'edw_connection'

    @staticmethod
    def get_default_schema():
        return 'prgedw'

    @staticmethod
    def get_s3_stage_uri(path, bucket=None, airflow_key="report_s3_access_key"):
        """
        returns smart_open compatible s3 uri from key
        :param bucket: bucket. If not specified default_bucket in the airflow key will be used.
        :param path: path. Path in the bucket.
        :param airflow_key: variable key name in airflow that contains the s3 access parameters.
        :return:
        """
        akey = json.loads(Variable.get(airflow_key))
        bucket = bucket or akey["default_bucket"]
        return get_s3_uri(bucket=bucket, path=path, aws_key=akey["aws_key"], aws_secret_key=akey["aws_secret_key"])
