from sender.test import *
from scheduler.test import *


class RunTest:

    data_dir = "data/examples"
    done_dir = "data/logs"

    @classmethod
    def test_scheduler(cls):
        TestEmailClassifier(cls.data_dir).test()
        TestJobScheduler(cls.data_dir).test()

    @classmethod
    def test_sender(cls):
        TestUser(cls.data_dir).test()
        TestMessage(cls.data_dir).test()
        TestBatchSenderBySimulator(cls.data_dir, cls.done_dir).test()


if __name__ == '__main__':
    RunTest.test_scheduler()
    RunTest.test_sender()




