import requests
import json
import logging
import os
import traceback
import time
from logging.handlers import TimedRotatingFileHandler

class CryptoNewsAPI:
    def __init__(self):
        self.auth_key = '##auth_key_tiingo##'
        self.api_url = 'https://api.tiingo.com/tiingo/news'
        self.log_dir = '/log/crypto_api'
        self.chk_dir()
        self.log = self._get_logger()

    def _get_logger(self):
        default_format = '%(asctime)s [%(levelname)s]:%(message)s'
        logging.basicConfig(format=default_format, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
        formatter = logging.Formatter(default_format)
        handler = TimedRotatingFileHandler(os.path.join(self.log_dir, 'call_crypto_api.log'), when="midnight", backupCount=7)
        handler.setFormatter(formatter)
        logger = logging.getLogger(__name__)
        logger.addHandler(handler)
        return logger

    def chk_dir(self):
        os.makedirs(self.log_dir, exist_ok=True)

    def call(self, tickers='btc,eth', limit=1000, start_date=None, end_date=None):
        headers = {'Content-Type': 'application/json', 'Authorization': f'Token {self.auth_key}'}
        params = {'tickers': tickers, 'limit': limit}
        
        if start_date:
            params['startDate'] = start_date
        if end_date:
            params['endDate'] = end_date # 과거 데이터를 페이징하기 위해 추가
        
        try:
            rslt = requests.get(self.api_url, headers=headers, params=params)
            try:
                contents = rslt.json()
            except Exception:
                self.log.error(f'요청 실패, {traceback.format_exc()}')
                time.sleep(30)
                return []

            if rslt.status_code != 200:
                self.log.error(f'요청 실패, 에러코드: {rslt.status_code}, 메시지: {rslt.text}')
                time.sleep(30)
                return []

            self.log.info(f'{self.api_url} 조회 성공, 건수: {len(contents)}')
            return contents
        except Exception:
            self.log.error(f'시스템 에러: {traceback.format_exc()}')
            return []