import requests
import logging
import time
from typing import Dict, Any
import json

from decc_automation.api.auth import AuthManager
from decc_automation.config.constants import DECC_V3_CONFIG

logger = logging.getLogger(__name__)

class DECCV3API:

    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0):
        self.session = requests.Session()
        self.auth = AuthManager()
        self.base_url = DECC_V3_CONFIG["base_url"]
        self.default_headers = DECC_V3_CONFIG.get("headers", {}).copy()
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def _make_request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        full_url = f"{self.base_url}{path}"
        
        # --- DEBUG LOG START ---
        print(f"\n[API REQUEST] {method} {full_url}")
        if 'params' in kwargs:
            print(f"[API REQUEST] Params: {json.dumps(kwargs['params'], ensure_ascii=False)}")
        if 'json' in kwargs:
            print(f"[API REQUEST] Body: {json.dumps(kwargs['json'], ensure_ascii=False)[:1000]}...") # Truncate long body
        # --- DEBUG LOG END ---

        headers = {**self.default_headers, **(kwargs.pop("headers", {}) or {})}
        lowered = {k.lower(): v for k, v in headers.items()}
        if "x-jwt-token" not in lowered and "authorization" not in lowered:
            headers["X-Jwt-Token"] = self.auth.get_token()
        headers.setdefault("domain", "decc_platform;v1")
        headers.setdefault("content-type", "application/json")

        try:
            debug_headers = {**headers}
            if 'Authorization' in debug_headers:
                debug_headers['Authorization'] = 'Bearer ***'
            if 'X-Jwt-Token' in debug_headers:
                debug_headers['X-Jwt-Token'] = '***'
            logger.debug(f"DECC Request: {method} {full_url} params={kwargs.get('params')} json={kwargs.get('json')} headers={debug_headers}")
        except Exception:
            pass

        last_exception = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.request(method, full_url, headers=headers, **kwargs)
                if response.status_code in [429, 500, 502, 503, 504]:
                    if attempt < self.max_retries:
                        wait_time = self.retry_delay * (2 ** attempt)
                        logger.warning(f"重试 {response.status_code}，{wait_time}s")
                        time.sleep(wait_time)
                        continue
                response.raise_for_status()
                if "application/json" not in response.headers.get("Content-Type", ""):
                    raise ValueError(f"非JSON响应: {response.text[:200]}")
                resp_json = response.json()
                
                # --- DEBUG LOG START ---
                print(f"[API RESPONSE] {response.status_code}")
                # Only log full response for get_data_list (which is critical here)
                if "/data/list" in path:
                    print(f"[API RESPONSE] Body: {json.dumps(resp_json, ensure_ascii=False)}")
                # --- DEBUG LOG END ---
                
                # 调试日志：打印响应概览
                try:
                    logger.debug(f"DECC Response: status={response.status_code} body={json.dumps(resp_json, ensure_ascii=False)[:2000]}")
                except Exception:
                    logger.debug(f"DECC Response (raw): status={response.status_code} body={response.text[:2000]}")
                return resp_json
            except requests.exceptions.RequestException as e:
                last_exception = e
                # 额外打印错误响应体，便于定位400等客户端错误
                try:
                    resp = getattr(e, 'response', None)
                    if resp is not None:
                        body = None
                        try:
                            body = resp.json()
                        except Exception:
                            body = resp.text
                        logger.debug(f"DECC Error Response: status={resp.status_code} body={body}")
                except Exception:
                    pass
                if attempt < self.max_retries:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"重试异常，{wait_time}s")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"请求失败: {e}")

    def get_channel_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """获取传输通道列表 """
        data = self._make_request("GET", DECC_V3_CONFIG["endpoints"]["get_channel_list"], params=params)
        logger.info(f"获取通道列表成功")
        return data.get("data", {})

    def get_data_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """获取数据列表 """
        data = self._make_request("GET", DECC_V3_CONFIG["endpoints"]["get_data_list"], params=params)
        logger.info(f"获取数据列表成功")
        return data.get("data", {})

    def get_data_version_detail(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """获取数据版本详情 """
        data = self._make_request("GET", DECC_V3_CONFIG["endpoints"]["get_data_version_detail"], params=params)
        logger.info(f"获取数据版本详情成功")
        return data.get("data", {})

    def update_data_version(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """更新数据版本"""
        resp = self._make_request("POST", DECC_V3_CONFIG["endpoints"]["update_data_version"], json=payload)
        logger.info(f"更新数据版本成功")
        return resp

    def create_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """创建数据"""
        resp = self._make_request("POST", DECC_V3_CONFIG["endpoints"]["create_data"], json=payload)
        logger.info(f"创建数据成功")
        return resp

    def submit_data_version(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """提交数据版本"""
        resp = self._make_request("POST", DECC_V3_CONFIG["endpoints"]["submit_data_version"], json=payload)
        logger.info(f"提交数据版本成功")
        return resp

    def create_data_version(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """创建数据草稿版本 """
        endpoint = DECC_V3_CONFIG.get("endpoints", {}).get("create_data_version", "/openapi/data_version/create")
        resp = self._make_request("POST", endpoint, json=payload)
        logger.info("创建数据草稿版本成功")
        return resp
