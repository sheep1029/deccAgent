"""
FaaS入口（v3）
接入 DECCFlowV3 主流程，支持批量与单表事件。

事件字段（JSON）：
- region: 字符串，支持逗号分隔（如 "EU" 或 "US,EU"）
- tables: 字符串，支持逗号分隔（如 "db.table" 或 "db1.t1,db2.t2"）
- operator: 执行人（映射为 owner）
- db_index: 可选整型，存在多个 channel 时按 channel_id 升序选择其索引
- additions: 可选，新增顶层字段 [{name,type,comment}]
- map_defs: 可选，MAP 子键定义 { field: { key: {type,comment} } }
- auto_submit: 可选布尔（目前忽略，按主流程既有逻辑执行）
"""

import json
import logging
import concurrent.futures
from typing import Any, Dict, List

from decc_automation.main.decc_flow import DECCFlowV3
from decc_automation.api.decc_v3_api import DECCV3API
from decc_automation.config.constants import GATEWAY, REGION_INPUT_TO_VGEO, SCENARIO_BY_VGEO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _split_comma(s: Any) -> List[str]:
    if not s:
        return []
    if isinstance(s, str):
        return [x.strip() for x in s.split(',') if x.strip()]
    return [str(s).strip()] if s else []


def _build_tasks(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    region_input = event.get('region')
    tables_input = event.get('tables')
    owner = event.get('owner')
    target_vgeo = event.get('target_vgeo')
    db_index = event.get('db_index')
    additions = event.get('additions') or []
    map_defs = event.get('map_defs') or {}
    auto_submit = bool(event.get('auto_submit', False))

    if not tables_input or not owner:
        raise ValueError("Missing required fields: 'tables', 'owner'.")

    regions = _split_comma(region_input) or ['TTP']
    tables = _split_comma(tables_input)
    tasks: List[Dict[str, Any]] = []
    for r in regions:
        for t in tables:
            if '.' not in t:
                raise ValueError(f"Invalid table format (expect 'db.table'): {t}")
            db_name, table_name = t.split('.', 1)
            tasks.append({
                'region_input': r,
                'channel_name': db_name,
                'data_name': f"{db_name}.{table_name}",
                'owner': owner,
                'db_index': db_index,
                'additions': additions,
                'map_defs': map_defs,
                'target_vgeo': target_vgeo,
                'auto_submit': auto_submit,
            })
    return tasks


def _run_single_task(flow: DECCFlowV3, task: Dict[str, Any]) -> Dict[str, Any]:
    try:
        result = flow.orchestrate_upsert_by_region(
            channel_name=task['channel_name'],
            data_name=task['data_name'],
            region_input=task['region_input'],
            target_vgeo=task['target_vgeo'],
            owner=task['owner'],
            additions=task['additions'],
            map_defs=task['map_defs'],
            db_index=task['db_index'],
        )
        # 统一包装成功响应，便于客户端格式化

        api = flow.api
        vgeo = REGION_INPUT_TO_VGEO.get(task['region_input'], 'ROW-TT')
        scenario = SCENARIO_BY_VGEO.get(vgeo, 5)
        # 选择 channel（按 channel_id 升序 + db_index）
        params = {
            'state': 1,
            'gateway': GATEWAY,
            'type': 5,
            'view_type': 2,
            'name': task['channel_name'],
            'owner': task['owner'],
            'page_number': 1,
            'page_size': 100,
        }
        channels = api.get_channel_list(params).get('channels', [])
        if not channels:
            # 退化：去掉 owner 过滤，避免过严导致为空
            params.pop('owner', None)
            channels = api.get_channel_list(params).get('channels', [])
        channel_id = None
        if channels:
            try:
                channels_sorted = sorted(channels, key=lambda c: int(c.get('channel_id')))
            except Exception:
                channels_sorted = channels
            idx = int(task['db_index']) if task.get('db_index') is not None else 0
            if 0 <= idx < len(channels_sorted):
                channel_id = channels_sorted[idx].get('channel_id')

        # 查询数据列表以获取 data_id
        data_id = None
        latest_version = None

        # 优先从 result 获取 version (避免列表延迟)
        if isinstance(result, dict):
            latest_version = result.get('version')

        if channel_id:
            table_name = task['data_name'].split('.', 1)[1]
            datas = api.get_data_list({
                'gateway': GATEWAY,
                'name': table_name,
                'channel_id': channel_id,
                'scenario': scenario,
                'page_number': 1,
                'page_size': 100,
            }).get('data', [])
            if datas:
                data_id = datas[0].get('data_id')
                # 仅当 result 中没有 version 时才使用列表中的 version
                if latest_version is None:
                    latest = (datas[0].get('latest_version_states') or {}).get(vgeo, {})
                    latest_version = latest.get('latestVersion')

        # 兜底：若未能通过列表拿到 data_id，则尝试使用主流程返回的 result 字段
        if not data_id:
            fallback_id = None
            try:
                # 优先从 result 顶层获取 (decc_flow 已保证注入)
                fallback_id = result.get('data_id')
                # 其次尝试从 data 字段获取
                if not fallback_id:
                    fallback_id = result.get('data')
                # 最后尝试从错误消息中正则提取
                if not fallback_id and isinstance(result.get('msg'), str):
                    import re
                    m = re.search(r"exist_data_id\s+is:\s*(\d+)", result['msg'])
                    if m:
                        fallback_id = m.group(1)
            except Exception:
                fallback_id = None
            if fallback_id:
                data_id = fallback_id

        # 可选自动提交：仅在拿到 latest_version 时执行
        auto_submit_info = {'requested': bool(task.get('auto_submit', False)), 'submitted': False}
        if task.get('auto_submit') and data_id and latest_version is not None:
            submit_payload = {
                'data_id': data_id,
                'version': latest_version,
                'gateway': GATEWAY,
                'scenario': scenario,
            }
            try:
                api.submit_data_version(submit_payload)
                auto_submit_info['submitted'] = True
            except Exception:
                auto_submit_info['submitted'] = False
                auto_submit_info['error'] = 'submit_failed'
        url = f"https://decc.tiktok-row.net/v3/des-hdfs/data?dataId={data_id}" if data_id else "N/A"

        return {
            'success': True,
            'region': task['region_input'],
            'db_name': task['channel_name'],
            'table_name': task['data_name'].split('.', 1)[1],
            'message': '操作成功',
            'result': result,
            'data_id': data_id,
            'url': url,
            'auto_submit': auto_submit_info,
        }
    except Exception as e:
        # 如果是结构化JSON错误（如多通道提示），直接透传该JSON
        msg = str(e).strip()
        if msg.startswith('{') and msg.endswith('}'):
            try:
                err_json = json.loads(msg)
                return err_json
            except Exception:
                pass
        return {
            'success': False,
            'region': task['region_input'],
            'db_name': task['channel_name'],
            'table_name': task['data_name'].split('.', 1)[1],
            'error': msg,
        }


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """FaaS入口（v3）"""
    headers = {'Content-Type': 'application/json'}
    try:
        if event.get('httpMethod') == 'POST':
            body = json.loads(event.get('body', '{}') or '{}')
        else:
            # 直接调用或其他入口
            body = event

        tasks = _build_tasks(body)
        flow = DECCFlowV3()

        # 单任务直接返回单个结果；多任务返回批量结构
        if len(tasks) == 1:
            result = _run_single_task(flow, tasks[0])
            # 服务端直接构造 Feishu 消息（单任务），客户端可直接透传
            try:
                if result.get('success'):
                    url = result.get('url') or 'N/A'
                    msg = result.get('message', '操作成功')
                    result['feishu_message'] = f"✅ {msg}\n链接: {url}"
                else:
                    err = result.get('error', '未知错误')
                    result['feishu_message'] = f"❌ 打标失败: {err}"
            except Exception:
                pass
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps(result, ensure_ascii=False, indent=2)
            }

        # 批量执行（改为线程池，并发数 10）
        results: List[Dict[str, Any]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_task = {executor.submit(_run_single_task, flow, task): task for task in tasks}
            for future in concurrent.futures.as_completed(future_to_task):
                try:
                    results.append(future.result())
                except Exception as exc:
                    t = future_to_task[future]
                    results.append({
                        'success': False,
                        'region': t.get('region_input'),
                        'db_name': t.get('channel_name'),
                        'table_name': t.get('data_name', '').split('.', 1)[1] if t.get('data_name') else '',
                        'error': f'Internal error: {exc}',
                    })

        summary = {
            'total_tasks': len(tasks),
            'successful': sum(1 for r in results if r.get('success')),
            'failed': sum(1 for r in results if not r.get('success')),
        }
        try:
            header = f"处理完成：总任务 {summary['total_tasks']} 个，成功 {summary['successful']} 个，失败 {summary['failed']} 个\n"
            success_messages = []
            for res in results:
                if res.get('success'):
                    table_id = f"{res.get('region', '')}.{res.get('db_name', '')}.{res.get('table_name', '')}"
                    url = res.get('url') or 'N/A'
                    success_messages.append(f"✅ {table_id}\n   链接: {url}")
            error_messages = []
            for res in results:
                if not res.get('success'):
                    table_id = f"{res.get('region', '')}.{res.get('db_name', '')}.{res.get('table_name', '')}"
                    error = res.get('error', '未知错误')
                    error_messages.append(f"❌ {table_id}\n   原因: {error}")
            final_message = header
            if success_messages:
                final_message += "--- 成功任务 ---\n" + "\n".join(success_messages)
            if error_messages:
                final_message += "--- 失败任务 ---\n" + "\n".join(error_messages)
            payload_for_feishu = {'results': results, 'summary': summary, 'feishu_message': final_message}
        except Exception:
            payload_for_feishu = {'results': results, 'summary': summary}
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(payload_for_feishu, ensure_ascii=False, indent=2)
        }

    except Exception as e:
        logger.error(f"[FATAL] {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({'success': False, 'error': str(e)}, ensure_ascii=False, indent=2)
        }
