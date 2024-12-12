from pathlib import Path

from scheduler import *
from sender import *


class Runner:

    jobs_dir = Path('data/jobs')
    logs_dir = Path('data/logs')
    raw_file = Path('data/raw/data.csv')
    jobs_done_path = logs_dir / 'jobs_done.csv'

    @classmethod
    def run_job_scheduler(cls, batch_size=100):

        if not cls.jobs_dir.exists():
            cls.jobs_dir.mkdir()

        js = JobScheduler(
            data_path=cls.raw_file,
            jobs_dir=cls.jobs_dir,
            done_path=cls.jobs_done_path,
            batch_size=batch_size
        )
        js.clear_job_files()
        js.assign_jobs()

    @classmethod
    def run_job_sender(cls, job_path, user_path, template_path):

        if not cls.logs_dir.exists():
            cls.logs_dir.mkdir()

        sender = BatchSender(
            user_path=user_path,
            template_path=template_path,
            receivers_path=job_path,
            done_path=cls.jobs_done_path
        )

        sender.run()

    @classmethod
    def run_sender(cls, user_path, template_path, receivers):

        if not cls.logs_dir.exists():
            cls.logs_dir.mkdir()

        if isinstance(receivers, str):
            receivers = [receivers]

        sender = BatchSender(
            user_path=Path(user_path),
            template_path=Path(template_path),
            receivers_path=None,
            done_path=cls.jobs_done_path,
            receivers=receivers,
        )

        sender.run()

