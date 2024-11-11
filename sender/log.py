import logging
from pathlib import Path
from datetime import datetime


class _Logger(object):

    _config = {
        'format': '[%(asctime)s][%(levelname)s]: %(message)s',
        'encoding': 'utf-8',
        'file_dir': 'data/logs',
        'logger_name': 'sender'
    }

    def __init__(self):

        logging.basicConfig(
            level=logging.INFO,  # 设置日志级别
            format=self._config['format'],  # 日志格式
            encoding=self._config['encoding'],
            handlers=[
                logging.FileHandler(self._logger_file_path),  # 日志写入文件
                logging.StreamHandler()  # 同时输出到控制台
            ]
        )

        logs_dir = Path(self._config['file_dir'])
        if not logs_dir.exists():
            logs_dir.mkdir()

        self._logger = logging.getLogger(self._config['logger_name'])

    @property
    def _logger_file_path(self):
        filename = ''.join([
            self._config['logger_name'],
            datetime.now().strftime("%Y%m%d"),
            '.log']
        )
        return Path(self._config['file_dir']) / filename

    def info(self, **kwargs):
        msg = [f"{k} = {v}" for k, v in kwargs.items()]
        self._logger.info(', '.join(msg))


logger = _Logger()