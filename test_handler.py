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
   "tables": "ad_dwa.dwa_ole_app_vo_signal_quantity_convert_stats_di_utc0",
   "owner": "zhuojinghao.1029"
#    "auto_submit": True,
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
        # 针对批量任务的响应结构处理
        summary = body.get('summary')
        if summary:
            print(f"\n📊 汇总: 总计 {summary.get('total_tasks')}, 成功 {summary.get('successful')}, 失败 {summary.get('failed')}")

        results = body.get('results', [])
        for res in results:
            region = res.get('region')
            success = res.get('success')
            url = res.get('url')
            data_id = res.get('data_id')
            submit_info = res.get('auto_submit', {})

            status_icon = "✅" if success else "❌"
            print(f"\n{status_icon} Region: {region}")
            print(f"   Success: {success}")
            if success:
                print(f"   Data ID: {data_id}")
                print(f"   URL: {url}")
                print(f"   Auto Submit: Requested={submit_info.get('requested')}, Submitted={submit_info.get('submitted')}")
            else:
                print(f"   Error: {res.get('error')}")

except Exception as e:
    print(f"\n❌ 发生未捕获异常: {e}")
    import traceback
    traceback.print_exc()
