from runner import Runner


def run_scheduler():
    r = Runner()
    r.run_job_scheduler()


def run_jobs_batch(name):
    """
    :param name: e.g. jobs_batch_0_0.csv
    """
    r = Runner()
    r.run_sender(job_name=name,
                 user='qiu.json',
                 template='template_qiu.txt')


if __name__ == '__main__':

    # run_jobs_batch('jobs_test.csv')
    pass



