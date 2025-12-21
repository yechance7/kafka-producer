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

    def _get_logger(self): # 로직 유지
        default_format = '%(asctime)s [%(levelname)s]:%(message)s'
        logging.basicConfig(format=default_format, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
        formatter = logging.Formatter(default_format)
        handler = TimedRotatingFileHandler(os.path.join(self.log_dir, 'call_crypto_api.log'), when="midnight", backupCount=7)
        handler.setFormatter(formatter)
        logger = logging.getLogger(__name__)
        logger.addHandler(handler)
        return logger

    def chk_dir(self): # 로직 유지
        os.makedirs(self.log_dir, exist_ok=True)

    def call(self, tickers='btc,eth', limit=100):
        # Tiingo API는 URL 파라미터나 헤더 모두 지원하지만 헤더가 보안상 권장됩니다.
        headers = {'Content-Type': 'application/json', 'Authorization': f'Token {self.auth_key}'}
        params = {'tickers': tickers, 'limit': limit}
        
        try:
            # _call_api 로직을 직접 구현
            rslt = requests.get(self.api_url, headers=headers, params=params)
            
            # JSONDecodeError 처리 로직 계승
            try:
                contents = rslt.json()
            except Exception:
                self.log.error(f'요청 실패, {traceback.format_exc()}')
                time.sleep(30)
                return []

            # HTTP 상태 코드 체크 (따릉이의 rslt_code 로직 대체)
            if rslt.status_code != 200:
                self.log.error(f'요청 실패, 에러코드: {rslt.status_code}, 메시지: {rslt.text}')
                time.sleep(30)
                return []

            self.log.info(f'{self.api_url} 조회 성공, 건수: {len(contents)}')
            return contents

        except Exception:
            self.log.error(f'시스템 에러: {traceback.format_exc()}')
            return []
        

if __name__ == '__main__':
    api = CryptoNewsAPI()
    result = api.call()
    print(result)