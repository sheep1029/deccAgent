
import json
import logging
import sys
import decc_automation
print(f"DEBUG: decc_automation path: {decc_automation.__file__}")
from index import handler

# 配置日志输出到控制台，确保能看到详细信息
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# 模拟的 Event 数据
test_event = {
   "region": "US",
   "tables": "ad_dwa.dwa_ole_promocode_creation_advertiser_v2_df_utc0",
   "owner": "zhuojinghao.1029"
}

print(f"🚀 开始执行测试，参数: {json.dumps(test_event, ensure_ascii=False)}")

try:
    # 直接调用 handler
    response = handler(test_event, None)

    print("\n✅ 执行完成！")
    print("--- 响应结果 ---")
    print(json.dumps(response, indent=2, ensure_ascii=False))

    # 检查是否有错误信息
    if response.get('statusCode') != 200:
        print("\n❌ 状态码非 200，执行可能失败")

    body = json.loads(response.get('body', '{}'))
    if isinstance(body, dict):
        if not body.get('success') and 'results' not in body:
             print(f"\n❌ 业务逻辑执行失败: {body.get('error')}")
        elif 'results' in body:
             for res in body['results']:
                 if not res.get('success'):
                     print(f"\n❌ 子任务失败: {res.get('table_name')} - {res.get('error')}")

except Exception as e:
    print(f"\n❌ 发生未捕获异常: {e}")
    import traceback
    traceback.print_exc()
