from slackclient import SlackClient
from airflow.operators.slack_operator import SlackAPIPostOperator as AirflowSlackAPIPostOperator
from airflow.models import Variable
import json


class SlackHook(object):
    def __init__(self):
        slack_details = json.loads(Variable.get('slack_de_access_token'))
        self.slack_token = slack_details["slack_token"]
        self.channel = slack_details["channel"]
        self.username = slack_details["user"]
        self.sc = SlackClient(self.slack_token)

    def send_slack_message(self, message, channel=None, attachments=None):
        """
        Post message to slack channel
        :param message: Message to be posted
        :param channel: channel to post
        :param attachments: list of json formatted attachments.
                      See https://api.slack.com/docs/message-attachments for details.
        :return:
        """
        sc = SlackClient(self.slack_token)
        at = ''
        sc.api_call(
            "chat.postMessage",
            channel=channel or self.channel,
            text=at + message,
            attachments=attachments,
            as_user=True
        )


class SlackAPIPostOperator(AirflowSlackAPIPostOperator):
    def __init__(self, message,
                 slack_access_key='slack_de_access_token',
                 icon_url='https://www.upgrade.com/LOGO-Green-Strips.svg',
                 attachments=None,
                 *args, **kwargs):
        slack_details = json.loads(Variable.get(slack_access_key))
        channel = slack_details["channel"]
        username = slack_details["user"]
        token = slack_details["slack_token"]
        super().__init__(token=token, channel=channel, username=username, text=message,
                         icon_url=icon_url, attachments=attachments, *args, **kwargs)
