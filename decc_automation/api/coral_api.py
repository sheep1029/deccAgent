import requests
import logging
from typing import Dict, Any

# 引入 AuthManager 和 配置
from decc_automation.api.auth import AuthManager
from decc_automation.config.constants import CORAL_CONFIG

logger = logging.getLogger(__name__)

class CoralAPI:

    def __init__(self):
        self.session = requests.Session()
        # 初始化认证管理器
        self.auth = AuthManager()

    def get_table_ddl(self, region: str, db_name: str, table_name: str) -> str:
        """
        获取 Hive 表的 DDL
        完全参考用户提供的有效 Python 脚本逻辑
        """

        # 1. 获取 User JWT (使用硬编码的用户名 zhuojinghao.1029，与参考代码一致)
        user_jwt = self.auth.get_user_token("zhuojinghao.1029")

        # 2. 构造请求
        url = f"{CORAL_CONFIG['base_url']}{CORAL_CONFIG['endpoints']['get_hive_ddl']}"

        params = {
            "cid": "6", # SG
            "dbName": db_name,
            "tableName": table_name
        }

        # 参考代码 headers
        headers = {
            "content-type": "application/json",
            "domain": "coral_openapi;v1",
            "x-jwt-token": user_jwt,
        }

        logger.info(f"正在从 Coral 获取 DDL: {db_name}.{table_name} (cid=6)...")

        try:
            # 发送请求
            response = self.session.get(url, headers=headers, params=params, timeout=30)

            # 错误处理逻辑
            if response.status_code != 200:
                logger.error(f"Coral API Error: {response.status_code} - {response.text}")
                response.raise_for_status()

            # 解析响应 (Coral API 通常返回 {"code": 0, "data": "CREATE TABLE ..."})
            # 注意：参考代码中直接打印了 response.text，未展示具体 JSON 结构解析
            # 根据经验和旧代码逻辑，通常结构为 {"code": 0, "data": ...}
            # 如果参考代码能跑通，说明响应体里直接包含了我们需要的信息

            data = response.json()
            if data.get("code") != 0:
                 raise Exception(f"Coral API 业务错误: {data.get('message')}")

            # 假设 data['data'] 就是 DDL 字符串，或者 data['data']['ddl']
            # 根据旧代码逻辑: ddl = data.get("data", {}).get("ddl")
            # 但有些 Coral 接口直接返回 data 字段为 DDL 字符串
            # 我们先尝试标准结构
            result_data = data.get("data")
            if isinstance(result_data, dict):
                ddl = result_data.get("ddl")
            else:
                ddl = str(result_data)

            if not ddl:
                raise Exception("API返回的DDL为空")

            logger.info(f"成功获取 DDL")
            return ddl

        except requests.RequestException as e:
            logger.error(f"获取DDL网络请求失败: {str(e)}")
            raise Exception(f"获取DDL失败: {str(e)}")

    def get_table_info(self, region: str, db_name: str, table_name: str) -> Dict[str, Any]:
        """
        获取表的基本信息

        Args:
            region: 区域代码
            db_name: 数据库名称
            table_name: 表名称

        Returns:
            Dict: 表信息
        """
        return {
            "db_name": db_name,
            "table_name": table_name,
            "region": region
        }
