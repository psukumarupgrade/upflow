from airflow.models import Variable
import re


class BaseSQLTagEvaluate(object):
    """
    Base Class for SQL Tag Evaluation
    """

    def get_val(self, key, default, enforce):
        raise NotImplementedError

    def __init__(self, sql, pattern, enforce=True, default=None):
        """
        Initialize the class and evaluates tags
        :param sql: input sql.
        :param pattern: tag name.
        :param enforce: enforce that tag has to be evaluated. Default is true.
        :param default: default value if tag cannot be evaluated and enforce is False.
        """
        self.sql = sql
        self.pattern = "<" + pattern + ">"
        compiled_regex = re.compile(self.pattern + ".+?" + self.pattern)
        sub_list = compiled_regex.findall(sql)
        for sub in sub_list:
            key = sub.replace(self.pattern, "")
            val = self.get_val(key, default, enforce)
            if val:
                self.sql = self.sql.replace(sub, val)

    def get_sql(self):
        return self.sql


class AirflowSQLTagEvaluate(BaseSQLTagEvaluate):
    """
    Airflow Class for SQL Tag Evaluation for pattern <airflow>XXX<airflow>
    """

    def get_val(self, key, default, enforce):
        try:
            return Variable.get(key)
        except:
            if enforce:
                raise
            else:
                return default or None

    def __init__(self, sql, default=None, enforce=True, **dummy):
        super(AirflowSQLTagEvaluate, self).__init__(sql, "airflow", default, enforce)


class CallbackSQLTagEvaluate(BaseSQLTagEvaluate):
    """
    Class for SQL Tag Evaluation for callback functions of pattern <callback>XXX<callback>
    """
    def get_val(self, key, default, enforce):
        try:
            if key != self.callback.__name__:
                return None
            else:
                if self.cargs and self.ckwargs:
                    return self.callback(*self.cargs, **self.ckwargs)
                elif self.cargs:
                    return self.callback(*self.cargs)
                elif self.ckwargs:
                    return self.callback(**self.ckwargs)
                else:
                    return self.callback()
        except:
            if enforce:
                raise
            else:
                return default or None

    def __init__(self, sql, default=None, enforce=True, **kwargs):
        if 'callback' in kwargs:
            self.callback = kwargs['callback']
            self.cargs = kwargs.get('callback_args_list', None)
            self.ckwargs = kwargs.get('callback_kwargs_dict', None)
            super(CallbackSQLTagEvaluate, self).__init__(sql, "callback", default, enforce)
        else:
            self.sql = sql


class KeyWordSQLTagEvaluate(BaseSQLTagEvaluate):
    """
    Class for SQL Tag Evaluation for pattern <key>XXX<key>
    """

    def get_val(self, key, default, enforce):
        try:
            return self.kwargs[key]
        except:
            if enforce:
                raise
            else:
                return default or None

    def __init__(self, sql, default=None, enforce=True, **kwargs):
        self.kwargs = kwargs
        super(KeyWordSQLTagEvaluate, self).__init__(sql, "keyword", default, enforce)


def sql_tag_evaluate(sql, default=None, enforce=True, skip_evaluators=None, evaluators=None, **kwargs):
    """
    Function to apply evaluators
    :param sql: sql to evaluate
    :param default: default value if enforce is false
    :param enforce: default True. Enforces evaluation of tags
    :param skip_evaluators: List of evaluator names to skip.
    :param evaluators: List of evaluator names in order to evaluate.
                            The default is KeyWordSQLTagEvaluate, CallbackSQLTagEvaluate,
                                           AirflowSQLTagEvaluate, HtAppSQLTagEvaluate, PlatformColumnsSQLTagEvaluate
    :param kwargs: Any other keyword arguments. Refer to individual sql evaluator classes for keywords.
    :return:
    """
    # Add evaluators to the evaluators list
    evaluators = evaluators or [KeyWordSQLTagEvaluate, CallbackSQLTagEvaluate, AirflowSQLTagEvaluate]
    skip_evaluators = skip_evaluators or []
    if not isinstance(skip_evaluators, list):
        skip_evaluators = [skip_evaluators]
    for evaluator in evaluators:
        if evaluator.__name__ not in skip_evaluators:
            sql = evaluator(sql, default, enforce, **kwargs).get_sql()
    return sql
