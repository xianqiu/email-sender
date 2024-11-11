import smtplib
from time import sleep
import csv
from datetime import datetime
from pathlib import Path

import socks

from .user import User
from .message import Message
from .log import logger


class EmailSender(object):

    def __init__(self, user_path, template_path, proxy=None):
        self._user = User(user_path)
        self._template_path = template_path
        self._smtp = smtplib.SMTP_SSL if self._user.ssl else smtplib.SMTP
        self._set_proxy(proxy)

    @staticmethod
    def _set_proxy(proxy):
        # 设置代理
        if not proxy:
            return
        host = proxy['host']
        if not host:
            return
        else:
            socks.set_default_proxy(socks.SOCKS5, host, proxy['port'])
            socks.wrapmodule(smtplib)

    def verify(self):
        try:
            with self._smtp(self._user.server, self._user.port) as server:
                server.login(self._user.email, self._user.password)
                print("User account is OK.")
        except smtplib.SMTPAuthenticationError:
            print("Incorrect email or password")
        except smtplib.SMTPConnectError:
            print("Can not connect to SMTP server")
        except Exception as e:
            print(f"Error：{e}")

    def _build_message(self, to):
        return Message(
            sender=self._user.email,
            receiver=to,
            template_path=self._template_path).build()

    def send(self, to):
        msg = self._build_message(to)
        try:
            with self._smtp(self._user.server, self._user.port) as server:
                server.login(self._user.email, self._user.password)
                server.send_message(msg)
            status = 'OK'
        except Exception as e:
            status = 'FAIL'
            print(f"Fail: {e}")

        return status


class EmailSenderSimulator(EmailSender):

    def send(self, to):
        try:
            self._build_message(to)
            sleep(0.1) # login
            sleep(0.5) # send
            status = 'OK'
        except Exception as e:
            print("Fail to build message", e)
            status = 'FAIL'
        return status


class BatchSender(object):

    _config = {
        'proxy': {
            'host': '127.0.0.1',
            'port': 7890
        },
        'gap_time': 10, # 两个任务之间的间隔时长，单位：秒
        'receivers': [],  # 用于测试，如果非空，则 self._receivers = receivers
        'simulate': False,  # 用于测试, True 模拟发送邮件
    }

    def __init__(self, user_path, template_path,
                 receivers_path, done_path,
                 **kwargs):

        self._user_path = user_path
        self._template_path = template_path
        self._receivers_path = receivers_path
        self._done_path = done_path

        for k, v in kwargs.items():
            if k in self._config.keys():
                self._config[k] = v
            else:
                raise ValueError(f"Wrong input parameter `{k}`!")

        self._receivers = []

    def _load_receivers(self):
        if self._config['receivers']:
            self._receivers = self._config['receivers']
            return
        try:
            with open(self._receivers_path, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    self._receivers.append(row['EMAIL'])
        except Exception as e:
            print(f"Error reading {self._receivers_path}: {e}")

    def _save_done(self, receiver, status):
        header = ['EMAIL', 'TIME', 'STATUS']
        if not Path(self._done_path).exists():
            with open(self._done_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows([header])

        row = [receiver, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), status]
        with open(self._done_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows([row])

    def run(self):
        self._load_receivers()
        logger.info(action="loading jobs", job_number=len(self._receivers))
        sender = EmailSender(self._user_path,
                             self._template_path,
                             self._config['proxy'])
        # 模拟发送邮件，用于测试
        if self._config['simulate']:
            sender = EmailSenderSimulator(self._user_path,
                             self._template_path,
                             self._config['proxy'])
        i = 0
        total = len(self._receivers)
        for re in self._receivers:
            status = sender.send(re)
            i += 1
            logger.info(receiver=re, status=status, process=f"{i}/{total}")
            self._save_done(re, status)
            sleep(self._config['gap_time'])
