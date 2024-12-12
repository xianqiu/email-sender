from runner import Runner


if __name__ == '__main__':
    # Runner.run_job_scheduler()

    user = 'data/user_qiu/qiu.json'
    template = 'data/user_qiu/letter.txt'

    # Runner.run_sender(user, template,[''])

    job_path = 'data/user_qiu/job_test.csv'
    Runner.run_job_sender(job_path, user, template)
