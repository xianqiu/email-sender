import csv
from pathlib import Path
import random
import os

from .email import EmailClassifier


class JobScheduler(object):

    """
    给定 Email 清单，数据格式为csv文件，Email对应的列名称为 'EMAIL'.
    把它们分成多个批次，满足如下条件：
    1、每个批次的任务数量不超过batch_size
    2、每个批次相同的域名数量不超过tolerance中指定的值。
    """

    _config = {
        'batch_size': 0,  # 0: unlimited
        'tolerance': {  # 每个批次允许域名相同的job数量
            'default': 1,  # 默认值
            'gmail.com': 3,  # 覆盖默认值
        },
    }

    def __init__(self, data_path, done_path, jobs_dir, **kwargs):
        """
        :param data_path: str, email 数据文件的路径，或者多个数据文件的路径列表
        :param done_path: str, email done 的文件路径，或者多个数据文件的路径列表
        :param jobs_dir: str, 结果保存的文件夹
        """
        for k, v in kwargs.items():
            if k in self._config.keys():
                self._config[k] = v
            else:
                raise (ValueError(f"Wrong input parameter `{k}`"))

        self._ec = EmailClassifier(data_path, done_path)
        self._jobs_dir = jobs_dir
        self._jobs = None

    def _get_tolerance(self, domain):
        tolerance = self._config['tolerance']
        val = tolerance['default']
        if domain.lower() in tolerance.keys():
            val = tolerance[domain]
        return val

    def _get_batches(self, classified_emails):
        """
        :param: classified_emails: dict,
            其中 key 是email的域名, vale 是相同域名下的email列表

        把 Email 按不同的域名分组，结果保存成 list。
        要求每个组中，域名相同的email数量不超过设定值 tolerance[key]，
        其中 key 是域名，例如 gmail.com。如果没有设定值，则不超过 tolerance[default]
        """
        res = []
        # 追踪self._jobs 中 key 对应的 value 是否为空
        # 如果 value 为空，则记录对应的 key 到 empty_keys
        # 因为下面的循环中要执行 pop 操作，直到 value 为空
        empty_keys = set()
        # 循环结束的条件：self._jobs 中所有的 value 都为空
        while len(empty_keys) < len(classified_emails):
            batch = []
            for key, values in classified_emails.items():
                if key in empty_keys:
                    continue
                count = 0
                tolerance = self._get_tolerance(key)
                while len(values) > 0 and count < tolerance:
                    if item := values.pop():
                            batch.append(item)
                            count += 1
                if len(values) == 0:
                    empty_keys.add(key)
            if batch:
                res.append(batch)
        return res

    def _get_jobs(self, batches):
        """
        格式化结果，e.g. {batch_id: jobs}, 每个batch的job数量不超过 batch_size。
        :return: dict
        """
        res = {}
        k = self._config['batch_size']
        if k == 0:  # 无限制
            k = max([len(b) for b in batches]) + 1
        for i in range(len(batches)):
            items = self._split_list(batches[i], k)
            if len(items) == 1:  # 1个batch
                random.shuffle(items[0])
                batch_id = f"{i}"
                res[batch_id] = items[0]
                continue
            for j in range(len(items)):
                random.shuffle(batches[j])
                batch_id = f"{i}_{j}"
                res[batch_id] = items[j]
        return res

    @staticmethod
    def _split_list(lst, k):
        """ 把输入列表lst按每k个元素进行拆分。
        例如: lst=[1,2,3,4,5,6,7], k=3
        返回结果为:[[1,2,3], [4,5,6],[7]]
        :param lst: list
        :param k: int
        :return: list, 二维列表
        """
        return [lst[i:i + k] for i in range(0, len(lst), k)]

    def _save_batch(self, batch_id, jobs):
        filename = f"jobs_batch_{batch_id}.csv"
        filepath = Path(self._jobs_dir) / filename
        try:
            with open(filepath, mode='w', newline='', encoding='utf-8') as f:
                header = ['EMAIL']
                writer = csv.DictWriter(f, fieldnames=header)
                # 写入列名
                writer.writeheader()
                # 写入数据
                writer.writerows([{'EMAIL': em} for em in jobs])
                print(f"Batch [{batch_id}] saved to [{filepath}].")
        except Exception as e:
            print(f"Save Failed! batch_id={batch_id}", e)

    def _save(self):
        for batch_id, jobs in self._jobs.items():
            self._save_batch(batch_id, jobs)
        self._print_count()

    def _count_jobs(self):
        removed = self._ec.get_emails_removed()
        count = {
            'ajn': 0,  # assigned job number
            'abn': len(self._jobs),  # assigned batch number
            'rcd': len(removed['cn']),  # removed cn domains
            'rjd': len(removed['done']),  # removed jobs done
            'rjn': 0,  # removed jobs number
        }
        for batch in self._jobs.values():
            count['ajn'] += len(batch)
        count['rjn'] = count['rcd'] + count['rjd']

        return count

    def _print_count(self):
        count = self._count_jobs()
        print("Jobs Assigned.\n"
              f">> jobs = {count['ajn']}, batches = {count['abn']}\n"
              f">> ignored = {count['rjn']}\n"
              f">> - cn domains ignored = {count['rcd']}\n"
              f">> - jobs done ignored = {count['rjd']}\n")

    def assign_jobs(self):
        classified_email = self._ec.classify()
        # 得到 batches = [[email, email, ...], [email, email, ...], ...]，满足 tolerance
        batches = self._get_batches(classified_email)
        # 得到 jobs = {batch_id: jobs}
        self._jobs = self._get_jobs(batches)
        self._save()

    def get_jobs(self):
        return self._jobs

    def clear_job_files(self):
        # 删除出 data_dir 文件下，文件名的格式为 jobs_batch_*.csv 的文件
        try:
            files = os.listdir(self._jobs_dir)
            for filename in files:
                if filename.startswith('jobs_batch_') and filename.endswith('.csv'):
                    os.remove(Path(self._jobs_dir) / filename)
                    print(f"File [{filename}] removed.")
        except Exception as e:
            print("Failed to remove job files", e)

