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
        "get_data_list": "/openapi/data/list",
        "get_data_version_detail": "/openapi/data_version/detail",
        "update_data_version": "/openapi/data_version/update",
        "create_data": "/openapi/data/create",
        "submit_data_version": "/openapi/data_version/submit",
        "create_data_version": "/openapi/data_version/create"
    }
}

import requests
import json
import logging
import threading
import time
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# =============================================================================
# 2. 获取 Token (服务账号)
# =============================================================================
def get_auth_token():
    """通过服务账号 Token 获取 X-Jwt-Token"""
    url = "https://cloud-i18n.bytedance.net/auth/api/v1/jwt"
    # 代码中写死的服务账号 Token (请确认这是 648fb... 还是您最新的 73bf...)
    # 暂时用 648fb 保持与原生一致，如需测试新账号请在此修改
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

        try:
            print(f"请求 API: {method} {full_url}")
            if kwargs.get('params'):
                print(f"  Query Params: {json.dumps(kwargs.get('params'), ensure_ascii=False)}")
            if kwargs.get('json'):
                print(f"  JSON Payload: {json.dumps(kwargs.get('json'), ensure_ascii=False)}")

            response = self.session.request(method, full_url, headers=headers, **kwargs)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                 print(f"  Error Body: {e.response.text[:500]}")
            return {}

    def get_channel_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """获取传输通道列表"""
        data = self._make_request("GET", DECC_V3_CONFIG["endpoints"]["get_channel_list"], params=params)
        print("✅ 获取通道列表成功")
        return data.get("data", {})

    def get_data_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """获取数据列表"""
        data = self._make_request("GET", DECC_V3_CONFIG["endpoints"]["get_data_list"], params=params)
        print("✅ 获取数据列表成功")
        return data.get("data", {})

    def get_data_version_detail(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """获取数据版本详情"""
        data = self._make_request("GET", DECC_V3_CONFIG["endpoints"]["get_data_version_detail"], params=params)
        print("✅ 获取数据版本详情成功")
        return data.get("data", {})

    def update_data_version(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """更新数据版本"""
        resp = self._make_request("POST", DECC_V3_CONFIG["endpoints"]["update_data_version"], json=payload)
        print("✅ 更新数据版本成功")
        return resp.get("data", {})

    def create_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """创建数据"""
        resp = self._make_request("POST", DECC_V3_CONFIG["endpoints"]["create_data"], json=payload)
        print("✅ 创建数据成功")
        return resp

    def submit_data_version(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """提交数据版本"""
        resp = self._make_request("POST", DECC_V3_CONFIG["endpoints"]["submit_data_version"], json=payload)
        print("✅ 提交数据版本成功")
        return resp

    def create_data_version(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """创建数据草稿版本"""
        endpoint = DECC_V3_CONFIG["endpoints"]["create_data_version"]
        resp = self._make_request("POST", endpoint, json=payload)
        print("✅ 创建数据草稿版本成功")
        return resp.get("data", {})

# =============================================================================
# 4. 测试入口
# =============================================================================
if __name__ == "__main__":
    print("=== 开始全量 API 测试 ===")
    api = DECCV3API()

    # 1. 测试 get_channel_list
    print("\n--- 1. 测试 get_channel_list ---")
    channel_params = {
        "state": 1,
        "gateway": GATEWAY,
        "view_type": 2,
        "name": "ad_dwa",
        "vgeo": "US",
        "owner": "zhuojinghao.1029",
        "page_number": 1,
        "page_size": 10
    }
    channels = api.get_channel_list(channel_params)
    print(f"结果: {json.dumps(channels, indent=2, ensure_ascii=False)[:500]}...") # 只打印前500字符

    # 2. 测试 get_data_list (使用 Channel ID: 7008798765637370159)
    # 使用写死的 Channel ID 进行测试，因为之前的测试表明 get_channel_list 可能会失败
    channel_id = "7008798765637370159"
    target_data_name = "dwa_ole_vo_signal_quantity_spearman_r7d_df_utc0"
    print(f"\n--- 2. 测试 get_data_list (Channel ID: {channel_id}, Table: {target_data_name}) ---")
    data_params = {
        "gateway": GATEWAY,
        "scenario": 2, # US 对应 2
        "channel_id": channel_id,
        "name": target_data_name,  # 增加 name 过滤
        "page_number": 1,
        "page_size": 10
    }
    datas = api.get_data_list(data_params)
    print(f"结果: {json.dumps(datas, indent=2, ensure_ascii=False)[:500]}...")

    # 3. 测试 get_data_version_detail (如果列表里有数据)
    # 注意：根据日志，API 返回的 key 是 "data" 而不是 "datas"
    if datas and datas.get("data"):
        # 找到匹配的数据
        target_data = None
        for d in datas["data"]:
             if d.get("name") == target_data_name:
                 target_data = d
                 break

        # 如果没有精确匹配，默认取第一个（可能就是它）
        if not target_data:
             target_data = datas["data"][0]

        data_id = target_data["data_id"]
        print(f"\n>> 选中数据: {target_data.get('name')} (ID: {data_id})")

        # 尝试获取 appliedVersion 或 editVersion
        # 注意：这里需要根据实际返回结构调整，假设 structure 如下
        # "data_version_states": { "US": { "appliedVersion": 123 } }
        states = target_data.get("data_version_states", {})
        # US 的 scenario 是 2，但在 states 里 key 可能是 "US" 字符串
        us_state = states.get("US", {})

        version = us_state.get("appliedVersion") or us_state.get("editVersion")

        if version:
            print(f"\n--- 3. 测试 get_data_version_detail (Data ID: {data_id}, Version: {version}) ---")
            version_params = {
                "data_id": data_id,
                "version": version,
                "gateway": GATEWAY,
                "scenario": 2
            }
            detail = api.get_data_version_detail(version_params)
            # 打印详细一些，看清楚字段
            print(f"结果: {json.dumps(detail, indent=2, ensure_ascii=False)[:2000]}...")

            # 4. 测试 create_data_version (创建草稿)
            # 基于获取到的详情，创建一个新的草稿版本
            print(f"\n--- 4. 测试 create_data_version (基于 Version {version} 创建草稿) ---")

            # 从 detail 中提取必要信息构建 payload
            # 注意：实际业务中可能需要清洗或修改这些字段
            base_info = detail.get("data_version", {})

            create_draft_payload = {
                "data_id": data_id,
                "gateway": GATEWAY,
                "scenario": 2, # US
                "based_version": version,
                "idl": base_info.get("idl", {}),
                "json_schema": base_info.get("json_schema", "{}"),
                "reason": "Automation Test Draft Creation",
                "description": "Created by test script for API verification"
            }

            print(">> 正在发送创建草稿请求...")
            try:
                # 真实执行写操作
                draft_resp = api.create_data_version(create_draft_payload)
                print(f"✅ 创建草稿成功! Response: {json.dumps(draft_resp, ensure_ascii=False)[:500]}...")
            except Exception as e:
                print(f"❌ 创建草稿失败: {e}")
                draft_resp = None

            # 5. 测试 update_data_version (更新草稿)
            # 如果上一步创建草稿成功，我们尝试更新这个草稿的描述信息
            # 注意：即使 draft_resp 为空（{}），只要不为 None，也说明请求成功了
            if 'draft_resp' in locals() and draft_resp is not None:
                # 解析新创建的 draft_version
                # 尝试多种可能的字段名
                draft_version = draft_resp.get("version") or draft_resp.get("id")

                # 如果返回为空但状态码成功，可能是因为 create 接口不返回 version，
                # 这种情况下我们需要重新 get_data_list 或 get_data_version_detail 来获取最新的 draft version
                if not draft_version:
                    print(">> 创建响应中未找到 version 字段，尝试重新查询数据获取最新草稿版本...")
                    # 重新查询数据列表
                    # 增加一点延迟，确保后端数据已更新
                    time.sleep(2)
                    datas_retry = api.get_data_list(data_params)
                    if datas_retry and datas_retry.get("data"):
                        for d in datas_retry["data"]:
                            if d.get("data_id") == data_id:
                                # 获取 editVersion
                                us_state_retry = d.get("data_version_states", {}).get("US", {})
                                # 优先取 editVersion，这通常是最新草稿
                                draft_version = us_state_retry.get("editVersion")
                                print(f">> 重新查询获取到的 Edit Version: {draft_version}")
                                break

                if draft_version:
                    print(f"\n--- 5. 测试 update_data_version (更新草稿 Version {draft_version}) ---")

                    update_payload = {
                        "data_id": data_id,
                        "version": draft_version, # 针对刚创建的草稿版本
                        "gateway": GATEWAY,
                        "scenario": 2,
                        "data_version": {
                            "description": f"Updated by test script at {time.strftime('%Y-%m-%d %H:%M:%S')}",
                            "reason": "Update test"
                            # 在实际更新中，通常还需要带上 idl/json_schema，否则可能会被置空
                            # 这里复用 base_info
                        }
                    }

                    # 补全 IDL 和 Schema，防止更新为空
                    update_payload["data_version"]["idl"] = base_info.get("idl", {})
                    update_payload["data_version"]["json_schema"] = base_info.get("json_schema", "{}")

                    print(">> 正在发送更新请求...")
                    try:
                        update_resp = api.update_data_version(update_payload)
                        print(f"✅ 更新草稿成功! Response: {json.dumps(update_resp, ensure_ascii=False)[:500]}...")
                    except Exception as e:
                        print(f"❌ 更新草稿失败: {e}")
                else:
                    print("⚠️ 无法获取新创建的草稿版本号，跳过更新测试")
            else:
                print("⚠️ 草稿创建失败或未执行，跳过更新测试")

        else:
            print("\n⚠️ 跳过版本详情测试：该数据没有 US 区域的版本信息")
    else:
        print(f"\n⚠️ 未找到名为 {target_data_name} 的数据")
