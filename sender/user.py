import json


class User(object):

    def __init__(self, filepath):
        self._filepath = filepath
        conf = self._load_conf()
        self.email = conf['email']
        self.password = str(conf['password'])
        self.server = conf['server']
        self.port = int(conf['port'])
        self.ssl = conf['ssl']

    def _load_conf(self):
        try:
            with open(self._filepath, 'r', encoding='utf-8') as file:
                data = json.load(file)  # 解析 JSON 数据
                return data
        except FileNotFoundError:
            print(f"{self._filepath} not found!")
            return None
        except Exception as e:
            print(f"File reading error: {e}")
            return None

