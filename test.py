from sender.test import *
from scheduler.test import *


def _test_scheduler():
    TestEmailClassifier().test()
    TestJobScheduler().test()


def _test_sender():
    TestUser().test()
    TestMessage().test()
    TestBatchSenderBySimulator().test()

def test_full():
    _test_scheduler()
    _test_sender()



