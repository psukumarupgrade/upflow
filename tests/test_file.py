from upflow.helpers.file import *
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class TestConfig(object):
    def setup(self):
        pass

    def teardown(self):
        pass

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def setup_method(self, method):
        print("Testing method:%s" % method.__name__)

    def teardown_method(self, method):
        pass

    def test_s3_uri_builder_with_access_key(self):
        scheme = "s3"
        user = "myuser"
        password = "mypassword"
        bucket = "mybucket"
        path = "mydir/mypath.txt"
        expected_output = "s3://myuser:mypassword@mybucket/mydir/mypath.txt"
        actual_output = get_s3_uri(bucket=bucket, aws_key=user, scheme=scheme, aws_secret_key=password, path=path)
        assert actual_output == expected_output

    def test_s3_uri_builder_without_access_key(self):
        scheme = "s3"
        user = None
        password = None
        bucket = "mybucket"
        path = "mydir/mypath.txt"
        expected_output = "s3://mybucket/mydir/mypath.txt"
        actual_output = get_s3_uri(bucket=bucket, aws_key=user, scheme=scheme, aws_secret_key=password, path=path)
        assert actual_output == expected_output

    def test_remove_access_key_from_uri_s3_with_access_key(self):
        uri = "s3://AKIAJNOVDEYB65OC4CQA:UH0rNbJUMj67p4utcP9uchBN/x10WR7HIP/Bb9y1@upgrade-poc-airflow/lir.csv"
        expected_output = "s3://upgrade-poc-airflow/lir.csv"
        actual_output = remove_access_key_from_uri(uri)
        assert actual_output == expected_output

    def test_remove_access_key_from_uri_s3_no_access_key(self):
        uri = "s3://upgrade-poc-airflow/lir.csv"
        expected_output = "s3://upgrade-poc-airflow/lir.csv"
        actual_output = remove_access_key_from_uri(uri)
        assert actual_output == expected_output

    def test_remove_access_key_from_uri_local(self):
        uri = "/data/upgrade-poc-airflow/lir.csv"
        expected_output = "/data/upgrade-poc-airflow/lir.csv"
        actual_output = remove_access_key_from_uri(uri)
        assert actual_output == expected_output
