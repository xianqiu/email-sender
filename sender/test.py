from pathlib import Path

from .message import Message
from .user import User
from .sender import EmailSender, BatchSender


__all__ = ['TestUser', 'TestMessage', 'TestEmailSender',
           'TestBatchSender', 'TestBatchSenderBySimulator']


class TestUser(object):

    """
    user_example.json
    {
        "email": "user@server.com",
        "password": "123456",
        "server": "smtp.server.com",
        "port": "465",
        "ssl": true
    }
    """

    def __init__(self, data_dir):
        data_dir = Path(data_dir)
        filepath = data_dir / 'user_example.json'
        self._user = User(filepath)

    def test(self):
        result = {
            'email': 'user@server.com',
            'name': 'Lucy',
            'password': '123456',
            'server': 'smtp.server.com',
            'port': 465,
            'ssl': True
        }
        assert self._user.email == result['email'], AssertionError(
            f"Email: got = {self._user.email}, expected = {result['email']}"
        )
        assert self._user.name == result['name'], AssertionError(
            f"Email: got = {self._user.name}, expected = {result['name']}"
        )
        assert self._user.password == result['password'], AssertionError(
            f"Email: got = {self._user.password}, expected = {result['password']}"
        )
        assert self._user.server == result['server'], AssertionError(
            f"Email: got = {self._user.server}, expected = {result['server']}"
        )
        assert self._user.port == result['port'], AssertionError(
            f"Email: got = {self._user.port}, expected = {result['port']}"
        )
        assert self._user.ssl == result['ssl'], AssertionError(
            f"Email: got = {self._user.ssl}, expected = {result['ssl']}"
        )
        print(f">> [{self.__class__.__name__}] OK.")


class TestMessage(object):

    """
    template_example.txt
    [SUBJECT]

    Hello World!

    [CONTENT]

    Nice to hear from you!

    Best wishes,
    sender
    """

    def __init__(self, data_dir):
        data_dir = Path(data_dir)
        filename = 'template_example.txt'
        self._template_path = data_dir / filename

    def test(self):
        sender = 'sender@example.com'
        receiver = 'receiver@example.com'
        sender_name = 'Lucy'
        message = Message(
            sender=sender,
            receiver=receiver,
            template_path=self._template_path,
            sender_name=sender_name
        )
        message.build()
        msg = message.get_msg()
        assert msg['From'] == f"{sender_name}<{sender}>", AssertionError(
            f"Sender: got = {msg['From']}, expected = {sender}"
        )
        assert msg['To'] == receiver, AssertionError(
            f"Receiver: got = {msg['To']}, expected = {receiver}"
        )
        subject = 'Hello, World!'
        assert msg['Subject'] == subject, AssertionError(
            f"Subject: got = {msg['Subject']}, expected = {subject}"
        )
        content = 'Nice to hear from you!\n\nBest wishes,\nsender'
        content_got = msg.get_payload(i=0).get_payload(decode=True).decode()
        assert content == content_got, AssertionError(
            f"Content: got = {content_got}, expected = {content}"
        )
        print(f">> [{self.__class__.__name__}] OK.")


class TestEmailSender(object):

    def __init__(self, user_path, template_path, to):
        proxy = {
            'host': '127.0.0.1',
            'port': 12334
        }
        self._sender = EmailSender(user_path, template_path, proxy)
        self._to = to

    def test(self):
        status = self._sender.send(self._to)
        if status == 'OK':
            print(f">> [{self.__class__.__name__}] OK.")


class TestBatchSenderBySimulator(object):

    def __init__(self, data_dir, done_dir, batch_size=10):
        data_dir, done_dir = Path(data_dir), Path(done_dir)
        user_path = data_dir / 'user_example.json'
        template_path = data_dir / 'template_example.txt'
        done_path = done_dir / 'jobs_done.csv'
        receivers = [f"user{i}@example.com" for i in range(batch_size)]
        self._bs = BatchSender(user_path=user_path,
                               template_path=template_path,
                               receivers_path=None,
                               done_path=done_path,
                               simulate=True,
                               receivers=receivers,
                               gap_time=0)

    def test(self):
        self._bs.run()
        print(f">> [{self.__class__.__name__}] OK.")


class TestBatchSender(object):

    def __init__(self, data_dir, done_dir, user_path, receivers):
        data_dir, done_dir = Path(data_dir), Path(done_dir)
        template_path = data_dir / 'template_example.txt'
        done_path = done_dir / 'jobs_done.csv'
        self._bs = BatchSender(user_path=user_path,
                               template_path=template_path,
                               receivers_path=None,
                               done_path=done_path,
                               receivers=receivers)

    def run(self):
        self._bs.run()
        print(f">> [{self.__class__.__name__}] OK.")

