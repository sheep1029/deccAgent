import requests
import logging
from typing import Dict, Any

from decc_automation.config.settings import API_HOST, AUTH_KEY

logger = logging.getLogger(__name__)

#通过Coral API获取表的DDL - 固定使用VA区域
class CoralAPI:
    
    def __init__(self):
        self.session = requests.Session()
        
    def get_table_ddl(self, region: str, db_name: str, table_name: str) -> str:
        # 固定使用VA区域的CID=1
        cid = 1
        
        url = f"{API_HOST}/openapi/new_coralng/v2/bridge/hive/ddl"
        params = {
            "cid": cid,
            "dbName": db_name,
            "tableName": table_name
        }
        headers = {
            "authorization": AUTH_KEY
        }
        
        try:
            response = self.session.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get("code") != 0:
                raise Exception(f"API返回错误: {data.get('message', '未知错误')}")
                
            ddl = data.get("data", {}).get("ddl")
            if not ddl:
                raise Exception("API返回的DDL为空")
                
            logger.info(f"成功获取 {db_name}.{table_name} 的DDL (使用VA区域CID=1)")
            return ddl
            
        except requests.RequestException as e:
            logger.error(f"获取DDL失败: {str(e)}")
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
