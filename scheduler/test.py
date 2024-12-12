from pathlib import Path

from .job import JobScheduler
from .email import EmailClassifier


__all__ = ['TestJobScheduler', 'TestEmailClassifier']


class TestEmailClassifier(object):

    def __init__(self, data_dir):
        data_dir = Path(data_dir)
        data_path = data_dir / 'data_example.csv'
        done_path = data_dir / 'jobs_done_example.csv'
        self._ec = EmailClassifier(data_path, done_path)

    def _test_removed_cn(self):
        removed_cn = self._ec.get_emails_removed()['cn']
        expected = ['user_cn0@163.com', 'user_cn1@126.com', 'user_cn2@qq.com',
                    'user_cn3@sina.com', 'user_cn4@sohu.com','user_cn5@example.com.cn',
                    'user_cn6@tycc.cn', 'user_cn7@huawei.com', 'user_cn8@example.edu.cn',
                    'user_cn9@example.org.cn', 'user_cn10@example.ac.cn']
        assert removed_cn == expected, AssertionError(
            f"CN domains removed: got = {removed_cn}, expected = {expected}"
        )

    def _test_removed_done(self):
        removed_done = self._ec.get_emails_removed()['done']
        expected = ['user1@example1.com', 'user4@example2.com']
        assert removed_done == expected, AssertionError(
            f"Emails done removed: got = {removed_done}, expected = {expected}"
        )

    def _test_result(self):
        res = self._ec.classify()
        expected = {
            'example1.com': ['user0@example1.com', 'user2@example1.com'],
            'example2.com': ['user3@example2.com', 'user5@example2.com']
        }
        if set(res.keys()) != set(expected.keys()):
            raise AssertionError(
            f"Keys: got = {res.keys()}, expected = {expected.keys()}"
        )
        for k, v in res.items():
            if set(res[k]) != set(expected[k]):
                raise AssertionError(
            f"Values: got = {v}, expected = {expected[k]}"
        )

    def test(self):
        self._test_removed_cn()
        self._test_removed_done()
        self._test_result()
        print(f">> [{self.__class__.__name__}] OK.")


class TestJobScheduler(object):

    def __init__(self, data_dir):
        data_dir = Path(data_dir)
        data_path = data_dir / 'data_example.csv'
        done_path = data_dir / 'jobs_done_example.csv'
        self._batch_size = 2
        self._tolerance = {'default': 1, 'example1.com': 2}
        self._sc = JobScheduler(
            data_path=data_path,
            done_path=done_path,
            jobs_dir=data_dir,
            batch_size=self._batch_size,
            tolerance=self._tolerance
        )
        self._jobs = None

    def _test_batch_size(self):
        for jobs in self._jobs.values():
            if len(jobs) > self._batch_size:
                raise AssertionError(f"Exceed batch size!\n"
                                     f">> jobs = {jobs}\n"
                                     f">> got size: {len(jobs)}, "
                                     f"expected size: {self._batch_size}")

    def _test_tolerance(self):
        for batch_id, jobs in self._jobs.items():
            count = {}
            for job in jobs:
                domain = job.split('@')[1]
                if domain not in count.keys():
                    count[domain] = 0
                count[domain] += 1

                tolerance = self._tolerance['default']
                if domain in self._tolerance.keys():
                    tolerance = self._tolerance[domain]
                if count[domain] > tolerance:
                    raise AssertionError(f"Exceed tolerance!\n"
                                         f">> domain={domain}\n"
                                         f">> got: {count[domain]}, "
                                         f"expected: {tolerance}")

    def _test_jobs(self):

        expected = {
            '0_0': ['user2@example1.com', 'user0@example1.com'],
            '0_1': ['user5@example2.com'],
            '1': ['user3@example2.com']
        }

        for batch_id in self._jobs.keys():
            if batch_id not in expected.keys():
                raise AssertionError(f"Key error! got={self._jobs.keys()}, "
                                     f"expected={expected.keys()}")

            if set(self._jobs[batch_id]) != set(expected[batch_id]):
                raise AssertionError(f"Batch jobs error!\n"
                                     f">> batch_id = {batch_id}"
                                     f">> got={self._jobs[batch_id]}\n "
                                     f">> expected={expected[batch_id]}\n")

    def test(self):
        self._sc.assign_jobs()
        self._jobs = self._sc.get_jobs()
        self._test_batch_size()
        self._test_tolerance()
        self._test_jobs()
        self._sc.clear_job_files()

        print(f">> [{self.__class__.__name__}] OK.")



