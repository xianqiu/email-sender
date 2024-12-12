import csv
from pathlib import Path


def _read_column(filepath, col):
    """
    从 csv 文件中读取一列（有标题列）
    :param filepath: 文件路径
    :param col: 列名
    :return: list
    """
    res = []
    if not Path(filepath).exists():
        return res
    try:
        with open(filepath, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            # 检查 EMAIL 列是否存在
            if col not in reader.fieldnames:
                return res
            for row in reader:
                item = row[col].strip()
                if item:  # 仅添加非空的item
                    res.append(item)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    return res


class EmailClassifier(object):

    """
    读取 Email 数据：csv格式，列字段为 `EMAIL`，按域名归类。
    返回格式为 dict, 其中 key 是域名，value 是对应email的列表。
    """
    _config = {
        'ignore_cn': True,
        'domains_cn': {'163.com', '126.com', 'qq.com', 'sina.com', 'sohu.com',  # 服务商
                       '.com.cn', 'tycc.cn', 'huawei.com',  # 企业
                       '.edu.cn', 'org.cn', '.ac.cn',  # 大学/组织
                       },
    }

    def __init__(self, data_path, done_path, **kwargs):
        """
        :param data_path: str, email 数据文件的路径，或者多个数据文件的路径列表
        :param done_path: str, email done 数据文件的路径或者多个数据文件的路径列表
        """
        for k, v in kwargs.items():
            self._config[k] = v
            if k in self._config.keys():
                self._config[k] = v
            else:
                raise (ValueError(f"Error key `{k}`"))

        self._email_list = self._load_email_list(data_path)
        self._emails_done = self._load_email_list(done_path)
        self._emails_removed = {
            'done': [],
            'cn': []
        }
        self._remove_emails_cn()
        self._remove_emails_done()

    def _load_email_list(self, data_path):
        """
        读取csv文件中的email数据。
        :param data_path: 单个文件的路径，或者多个文件的路径列表
        返回 `EMAIL` 列的数据。
        """
        if isinstance(data_path, list):
            self._data_paths = data_path
        else:
            self._data_paths = [data_path]

        email_list = []
        for filepath in self._data_paths:
            email_list += _read_column(filepath, col='EMAIL')
        return email_list

    def _remove_emails_cn(self):
        if not self._config['ignore_cn']:
            return
        kept = []
        removed = []
        for em in self._email_list:
            if self._is_cn_domain(em):
                removed.append(em)
            else:
                kept.append(em)

        self._email_list = kept
        self._emails_removed['cn'] = removed

    def _remove_emails_done(self):
        if not self._emails_done:
            return
        kept = []
        removed = []
        for em in self._email_list:
            if em in self._emails_done:
                removed.append(em)
            else:
                kept.append(em)

        self._email_list = kept
        self._emails_removed['done'] = removed

    def _is_cn_domain(self, email):
        domains_cn = self._config['domains_cn']
        for domain in domains_cn:
            if domain in email:
                return True
        return False


    def classify(self):
        """ 按 email 域名进行分类，结果保存为 dict
            key 是 email 域名，
            value 是 list，包括域名相同的所有的 email
        """
        res = {}
        for email in self._email_list:
            suffix = email.lower().split('@')[1].strip()
            if suffix not in res:
                res[suffix] = []
            res[suffix].append(email)

        return res

    def get_emails_removed(self):
        return self._emails_removed