import json
import logging
import sys
import os
import urllib.parse

# 确保能找到 decc_automation 模块
sys.path.append(os.getcwd())

from decc_automation.api.decc_v3_api import DECCV3API
from decc_automation.config.constants import GATEWAY

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_submit_version(data_id, version, scenario):
    """
    测试提交指定的数据版本
    """
    api = DECCV3API()

    print(f"\n{'='*20} Testing Submit Version {'='*20}")
    print(f"Data ID: {data_id}")
    print(f"Version: {version}")
    print(f"Scenario: {scenario}")

    payload = {
        "data_id": data_id,
        "version": version,
        "gateway": GATEWAY,
        "scenario": scenario
    }

    # 构造 Curl 命令
    base_url = "https://paas-gw-us.byted.org"
    full_url = f"{base_url}/openapi/data_version/submit"

    print(f"[SUBMIT REQUEST] URL: {full_url}")
    print(f"[SUBMIT REQUEST] Body: {json.dumps(payload, ensure_ascii=False)}")
    print(f"[SUBMIT REQUEST] Curl Command:")
    print(f"curl -X POST '{full_url}' \\")
    print(f"     -H 'Content-Type: application/json' \\")
    print(f"     -H 'domain: decc_platform;v1' \\")
    print(f"     -H 'X-Jwt-Token: YOUR_TOKEN_HERE' \\")
    print(f"     -d '{json.dumps(payload, ensure_ascii=False)}'")

    try:
        response = api.submit_data_version(payload)
        print(f"✅ [SUBMIT RESPONSE] Success! Body: {json.dumps(response, ensure_ascii=False, indent=2)}")
    except Exception as e:
        print(f"❌ [SUBMIT RESPONSE] Failed: {e}")

def test_get_data_list():
    api = DECCV3API()
    # 替换为您要测试的表名
    # table_name = "dwa_ole_promocode_creation_advertiser_v2_df_utc0"
    table_name = "dm_web_ole_ad_label_df_utc0" # 换成这个好表

    from decc_automation.main.decc_flow import DECCFlowV3
    flow = DECCFlowV3()
    owner = "zhuojinghao.1029"
    channel_name = "ad_dm"

    # 1. 遍历 US 和 EU 进行测试
    regions = [
        # {"vgeo": "US", "scenario": 2},
        {"vgeo": "EU", "scenario": 3}
    ]

    for region_info in regions:
        vgeo = region_info["vgeo"]
        scenario = region_info["scenario"]

        print(f"\n{'='*20} Debugging Channels for {channel_name} ({vgeo}) {'='*20}")
        list_params = {
            "gateway": GATEWAY,
            "type": 5,
            "view_type": 2,
            "name": channel_name,
            "vgeo": vgeo,
            "owner": owner,
            "page_number": 1,
            "page_size": 100,
        }
        print(f"  [CHANNEL REQUEST] Params: {json.dumps(list_params, ensure_ascii=False)}")
        channels_resp = api.get_channel_list(list_params)
        print(f"  [CHANNEL RESPONSE] Body: {json.dumps(channels_resp, ensure_ascii=False, indent=2)}")
        channels = channels_resp.get("channels", [])
        print(f"Found {len(channels)} channels in {vgeo}:")

        valid_channel_id = None

        for i, ch in enumerate(channels):
            cid = ch.get("channel_id")
            cname = ch.get("name")
            print(f"  [{i}] ID: {cid}, Name: {cname}, Region: {ch.get('region')}")

            # 尝试用这个 Channel ID 去查数据
            print(f"      Checking data with Channel ID: {cid}...")
            d_params = {
                "gateway": GATEWAY,
                "name": table_name,
                "scenario": scenario,
                "channel_id": cid,
                "page_number": 1,
                "page_size": 100
            }
            # 构造完整的 Curl 命令
            base_url = "https://paas-gw-us.byted.org"
            full_url = f"{base_url}/openapi/data/list"

            # 手动构造 query string
            import urllib.parse
            query_string = urllib.parse.urlencode(d_params)
            full_request_url = f"{full_url}?{query_string}"

            print(f"      [DATA REQUEST] Method: GET")
            print(f"      [DATA REQUEST] URL: {full_request_url}")
            print(f"      [DATA REQUEST] Headers: (Hidden for security, contains X-Jwt-Token)")
            print(f"      [DATA REQUEST] Curl Command:")
            print(f"      curl -X GET '{full_request_url}' \\")
            print(f"           -H 'Content-Type: application/json' \\")
            print(f"           -H 'domain: decc_platform;v1' \\")
            print(f"           -H 'X-Jwt-Token: YOUR_TOKEN_HERE'")

            try:
                d_resp = api.get_data_list(d_params)
                print(f"      [DATA RESPONSE] Full Body ({vgeo}): {json.dumps(d_resp, ensure_ascii=False, indent=2)}")
                d_list = d_resp.get("data", [])
                if d_list:
                    data_item = d_list[0]
                    data_id = data_item.get('data_id')
                    print(f"      ✅ Data FOUND! ID: {data_id}")
                    valid_channel_id = cid

                    # 获取最新版本号
                    latest_states = data_item.get('latest_version_states', {})
                    if latest_states:
                        # 尝试获取当前 region 的 version，如果拿不到就拿第一个 available 的
                        region_state = latest_states.get(vgeo) or list(latest_states.values())[0]
                        latest_version = region_state.get('latestVersion')
                        print(f"      Latest Version: {latest_version}")

                        # 调用提交函数测试提交这个最新版本
                        # 注意：如果这个版本已经是 submitted 状态，再次提交可能会报错或无操作
                        if latest_version:
                            test_submit_version(data_id, latest_version, scenario)
                    else:
                        print("      ⚠️ No latest_version_states found, cannot test submit.")

                else:
                    print(f"      ❌ No data found.")
            except Exception as e:
                print(f"      ❌ Error: {e}")

        if valid_channel_id:
            print(f"\n🎯 Conclusion for {vgeo}: The working Channel ID is {valid_channel_id}")
        else:
            print(f"\n❌ Conclusion for {vgeo}: No working Channel ID found in the list.")

def run_standalone_submit_test():
    """
    单独测试提交功能，使用已知的 Data ID 和 Version
    """
    print(f"\n{'#'*20} Running Standalone Submit Test {'#'*20}")

    # 这里填入您之前运行获取到的真实信息
    # 例如从 test_handler.py 的日志中获取
    data_id = "7615088658168693048" # EU data id
    version = 13 # 刚刚创建成功的版本
    scenario = 3 # EU

    test_submit_version(data_id, version, scenario)

if __name__ == "__main__":
    # test_get_data_list()
    run_standalone_submit_test()
    # test_new_api_for_eu_version()
    # test_resolve_channel_id()
    # test_production_logic()