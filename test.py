# =============================================================================
# 1. 配置文件
# =============================================================================

# 网关ID
GATEWAY = 6

DECC_V3_CONFIG = {
    "base_url": "https://paas-gw-us.byted.org",
    "headers": {
        'origin': 'https://cloud.bytedance.net',
        'referer': 'https://cloud.bytedance.net/',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'cache-control': 'no-cache',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'domain': 'decc_platform;v1',
    },
    "endpoints": {
        "get_channel_list": "/openapi/channel/list",
    }
}

import requests
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# =============================================================================
# 2. 获取 Token (新增函数)
# =============================================================================
def get_auth_token():
    """通过服务账号 Token 获取 X-Jwt-Token"""
    url = "https://cloud-i18n.bytedance.net/auth/api/v1/jwt"
    # 代码中写死的服务账号 Token
    headers = {"Authorization": "Bearer 648fb17dd9052d5183d554762e347fef"}

    print(f"正在获取 Token: {url}...")
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()

        token = resp.headers.get("X-Jwt-Token")
        if not token:
            print("❌ 获取失败: 响应头中没有 X-Jwt-Token")
            return None

        print("✅ 成功获取 X-Jwt-Token")
        return token
    except Exception as e:
        print(f"❌ 获取 Token 出错: {e}")
        return None

# =============================================================================
# 3. API 客户端
# =============================================================================
class DECCV3API:
    def __init__(self):
        self.session = requests.Session()
        # 原生代码中这里会初始化 AuthManager，为简化展示，此处省略 AuthManager 细节
        # self.auth = AuthManager()
        self.base_url = DECC_V3_CONFIG["base_url"]
        self.default_headers = DECC_V3_CONFIG.get("headers", {}).copy()

    def _make_request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        full_url = f"{self.base_url}{path}"
        headers = {**self.default_headers, **(kwargs.pop("headers", {}) or {})}

        # 自动获取并注入 X-Jwt-Token
        lowered = {k.lower(): v for k, v in headers.items()}
        if "x-jwt-token" not in lowered and "authorization" not in lowered:
             token = get_auth_token()
             if token:
                 headers["X-Jwt-Token"] = token

        headers.setdefault("domain", "decc_platform;v1")
        headers.setdefault("content-type", "application/json")

        # ... (省略了原生代码中的日志打印和重试逻辑) ...

        response = self.session.request(method, full_url, headers=headers, **kwargs)
        response.raise_for_status()
        return response.json()

    def get_channel_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """获取传输通道列表 """
        data = self._make_request("GET", DECC_V3_CONFIG["endpoints"]["get_channel_list"], params=params)
        logger.info("获取通道列表成功")
        return data.get("data", {})

# =============================================================================
# 4. 业务调用流程
# =============================================================================

class DECCFlowV3:
    def __init__(self):
        self.api = DECCV3API()

    def _resolve_channel_id(self, channel_name: str, vgeo: str, owner: str,
                            db_index: Optional[int]) -> str:
        # 构造初始参数
        params = {
            "state": 1,
            "gateway": GATEWAY,
            "type": 5,
            "view_type": 2,
            "name": channel_name,
            "vgeo": vgeo,
            "owner": owner,
            "page_number": 1,
            "page_size": 100,
        }

        # 第一次尝试：全参数查询
        channels = self.api.get_channel_list(params).get("channels", [])

        # 第二次尝试：去掉 Owner
        if not channels:
            params.pop("owner", None)
            channels = self.api.get_channel_list(params).get("channels", [])

        # 第三次尝试：去掉 Vgeo
        if not channels:
            params.pop("vgeo", None)
            channels = self.api.get_channel_list(params).get("channels", [])

        # 第四次尝试：最小参数兜底
        if not channels:
            minimal = {"state": 1, "gateway": GATEWAY}
            channels = self.api.get_channel_list(minimal).get("channels", [])

        if not channels:
            raise ValueError(f"Channel not found: {channel_name}")

        # 结果排序
        try:
            channels_sorted = sorted(channels, key=lambda c: int(c.get("channel_id")))
        except Exception:
            channels_sorted = channels

        # 根据 index 选择
        if db_index is not None:
            if not (0 <= int(db_index) < len(channels_sorted)):
                raise ValueError(f"Invalid db_index {db_index}, available range: 0..{len(channels_sorted)-1}")
            return channels_sorted[int(db_index)]["channel_id"]

        # 多结果检查
        if len(channels_sorted) >= 2:
            listing = [
                {
                    "db_index": i,
                    "channel_id": ch.get("channel_id"),
                    "name": ch.get("name")
                } for i, ch in enumerate(channels_sorted)
            ]
            raise ValueError(json.dumps({
                "code": "MULTIPLE_CHANNELS",
                "msg": f"Found multiple channels for '{channel_name}'. Please set db_index.",
                "channels": listing
            }, ensure_ascii=False))

        # 默认返回第一个
        return channels_sorted[0]["channel_id"]

# =============================================================================
# 5. 测试执行入口
# =============================================================================

if __name__ == "__main__":
    # 配置测试参数
    TARGET_CHANNEL_NAME = "ad_dwa"
    TARGET_OWNER = "zhuojinghao.1029"
    TARGET_VGEO = "US"

    print("=== 开始测试 DECC 自动化流程 (原生逻辑) ===")

    try:
        # 1. 初始化 Flow
        flow = DECCFlowV3()

        # 2. 调用核心逻辑解析 Channel ID
        print(f"\n正在解析 Channel ID... \nName: {TARGET_CHANNEL_NAME}, Owner: {TARGET_OWNER}, Vgeo: {TARGET_VGEO}")

        channel_id = flow._resolve_channel_id(
            channel_name=TARGET_CHANNEL_NAME,
            vgeo=TARGET_VGEO,
            owner=TARGET_OWNER,
            db_index=None
        )

        print(f"\n✅ 测试成功！")
        print(f"解析到的 Channel ID: {channel_id}")

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")