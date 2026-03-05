import requests
from typing import Optional
import logging
import time
import os
import threading

from decc_automation.config.settings import TOKEN_URL, TOKEN_AUTH

logger = logging.getLogger(__name__)

# 服务账号JWT有效期为1小时，设置刷新机制
TOKEN_LIFETIME_SECONDS = 55 * 60
class AuthManager:

    _instance: Optional["AuthManager"] = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return
        self._token: Optional[str] = None
        self._token_timestamp: float = 0.0
        self._fetch_lock = threading.Lock()
        self._initialized = True

    def _is_token_expired(self) -> bool:
        """检查缓存的token是否已超过其生命周期"""
        if not self._token:
            return True
        
        elapsed_time = time.time() - self._token_timestamp
        is_expired = elapsed_time > TOKEN_LIFETIME_SECONDS
        if is_expired:
            logger.info(f"Token has expired after {elapsed_time:.0f} seconds (lifetime: {TOKEN_LIFETIME_SECONDS}s).")
        return is_expired

    def get_token(self) -> str:
        """
        获取JWT token。如果token不存在或已过期，则自动获取新的token。
        
        Returns:
            str: 有效的JWT token
            
        Raises:
            Exception: 获取token失败时抛出异常
        """

        # 快速路径：已有且未过期，直接返回
        if self._token and not self._is_token_expired():
            logger.info("Using valid cached JWT token.")
            return self._token

        with self._fetch_lock:
            if self._token and not self._is_token_expired():
                logger.info("Using valid cached JWT token.")
                return self._token

            logger.info("Token is missing or expired, fetching a new one...")
            try:
                headers = {"Authorization": TOKEN_AUTH}
                # 为token请求本身也设置一个超时
                response = requests.get(TOKEN_URL, headers=headers, timeout=15)
                response.raise_for_status()

                token = response.headers.get("X-Jwt-Token")
                if not token:
                    raise Exception("获取token失败: 响应中没有X-Jwt-Token头")

                self._token = token
                self._token_timestamp = time.time()
                logger.info("成功获取并缓存了新的JWT token")
                return token

            except requests.RequestException as e:
                logger.error(f"获取token失败: {str(e)}")
                # 如果获取失败，清除旧的token信息，以确保下次会重试
                self.clear_token()
                raise Exception(f"获取token失败: {str(e)}")
    
    def clear_token(self):
        """清除缓存的token和时间戳"""
        self._token = None
        self._token_timestamp = 0.0
        logger.info("Cached token has been cleared.")
