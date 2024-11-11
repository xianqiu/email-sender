from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re


class Message(object):

    def __init__(self, sender, receiver, template_path):
        self._sender = sender
        self._receiver = receiver
        self._template_path = template_path
        self._subject = ''
        self._content = ''
        self._msg = MIMEMultipart()

    def _load_template(self):
        if not self._template_path:
            return
        try:
            with open(self._template_path, 'r', encoding='utf-8') as file:
                res = self._parse_template(file.read())
                self._subject = res['SUBJECT']
                self._content = res['CONTENT']
        except FileNotFoundError:
            print(f"File not found: {self._template_path}")
        except Exception as e:
            print(f"File reading error: {e}")

    @staticmethod
    def _parse_template(text):
        """
        给定文本text，其中的章节用`[]`扩起来，返回章节对应的文本内容。
        例如:
        [SUBJECT]
        Hello!

        [CONTENT]
        World!

        返回
        {
            'SUBJECT': 'Hello!,
            'CONTENT': 'World!'
        }
        注意：需要去掉内容中第一行和最后一行的空格和换行符。
        """
        # 使用正则表达式匹配章节和内容
        pattern = r'\[(.*?)\]\s*(.*?)\s*(?=\[\w+\]|$)'

        matches = re.findall(pattern, text, re.DOTALL)
        result = {}

        for title, content in matches:
            # 去掉内容的首尾空格和换行符
            result[title.strip()] = content.strip()

        return result

    @staticmethod
    def _is_valid_email(email):
        # 定义正则表达式
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None

    def _check(self):
        if not self._is_valid_email(self._sender):
            raise ValueError(f"{self._sender} is not valid!")
        if not self._is_valid_email(self._receiver):
            raise ValueError(f"{self._receiver} is not valid!")
        if not self._subject:
            raise ValueError(f"Empty subject!")
        if not self._content:
            raise ValueError(f"Empty content!")

    def build(self):
        self._load_template()
        self._msg['From'] = self._sender
        self._msg['To'] = self._receiver
        self._msg['Subject'] = self._subject
        self._check()
        self._msg.attach(MIMEText(self._content, 'plain', 'utf-8'))
        return self._msg

    def get_msg(self):
        return self._msg