from pathlib import Path

from scheduler import *
from sender import *


class Runner(object):

    def __init__(self):
        self._data_dir = Path('data')
        self._jobs_dir = self._data_dir / 'jobs'

        if not self._data_dir.exists():
            self._data_dir.mkdir()
        if not self._jobs_dir.exists():
            self._jobs_dir.mkdir()

    def run_job_scheduler(self, filename='data.csv', batch_size=100):

        jobs_done_filename = 'jobs_done.csv'

        js = JobScheduler(
            data_path=self._data_dir / filename,
            done_path=self._jobs_dir / jobs_done_filename,
            jobs_dir=self._jobs_dir,
            batch_size=batch_size
        )
        js.clear_job_files()
        js.assign_jobs()

    def run_sender(self, job_name, user, template):

        user_path = self._data_dir / user
        template_path = self._data_dir / template
        receivers_path = self._jobs_dir / job_name
        done_path = self._jobs_dir / 'jobs_done.csv'

        sender = BatchSender(
            user_path=user_path,
            template_path=template_path,
            receivers_path=receivers_path,
            done_path=done_path
        )

        sender.run()


