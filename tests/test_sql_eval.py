from upflow.helpers.sql import *
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

    def test_sql_keyword_evaluator(self):
        input_sql = "select * from <keyword>table<keyword>;"
        expected_output_sql = "select * from temp_table;"
        sql_eval_tag_dict = {"table": "temp_table"}
        actual_output_sql = KeyWordSQLTagEvaluate(sql=input_sql, **sql_eval_tag_dict).get_sql()
        assert actual_output_sql == expected_output_sql

    def test_sql_callback_evaluator(self):
        def crop(text, start):
            return text[start:]
        fnc = crop
        fnc_arg = ["hello"]
        fnc_kwargs = {"start": 3}
        input_sql = "select * from table where stuff = '<callback>crop<callback>';"
        expected_output_sql = "select * from table where stuff = 'lo';"
        actual_output_sql = CallbackSQLTagEvaluate(sql=input_sql, callback=fnc,
                                                   callback_args_list=fnc_arg,
                                                   callback_kwargs_dict=fnc_kwargs).get_sql()
        assert actual_output_sql == expected_output_sql

    def test_sql_callback_evaluator_no_arguments(self):
        def capitalize():
            return "HelLo".upper()
        fnc = capitalize
        input_sql = "select * from table where stuff = '<callback>capitalize<callback>';"
        expected_output_sql = "select * from table where stuff = 'HELLO';"
        actual_output_sql = CallbackSQLTagEvaluate(sql=input_sql, callback=fnc).get_sql()
        assert actual_output_sql == expected_output_sql

    def test_sql_multi_evaluator(self):
        def crop(text, start):
            return text[start:]

        fnc = crop
        fnc_arg = ["hello"]
        fnc_kwargs = {"start": 3}
        sql_eval_tag_dict = {"table": "temp_table"}
        input_sql = "select * from <keyword>table<keyword> where stuff = '<callback>crop<callback>';"
        expected_output_sql = "select * from temp_table where stuff = 'lo';"
        actual_output_sql = sql_tag_evaluate(sql=input_sql, callback=fnc,
                                             callback_args_list=fnc_arg,
                                             callback_kwargs_dict=fnc_kwargs,
                                             **sql_eval_tag_dict)
        assert actual_output_sql == expected_output_sql
        sql_eval_tag_dict = {
            "table": "temp_table",
            "callback": fnc,
            "callback_args_list": fnc_arg,
            "callback_kwargs_dict": fnc_kwargs
        }
        actual_output_sql = sql_tag_evaluate(sql=input_sql, **sql_eval_tag_dict)
        assert actual_output_sql == expected_output_sql
