import logging
import re
import json
from typing import Dict, Any, List, Optional

from decc_automation.config.constants import (
    DECC_V3_DATA_TYPE, DECC_V3_IDL_TYPE, GATEWAY, REASON
)
from decc_automation.api.decc_v3_api import DECCV3API

logger = logging.getLogger(__name__)

class DataVersionService:
    """数据版本服务层：构造update/create/submit的payload"""

    def __init__(self):
        pass

    def _compose_version_content(self, vgeo: str, scenario: int,
                                  version: int, upstream_version: int,
                                  reason: str, extra: Dict[str, Any],
                                  ddl: str, schema_str: str,
                                  nested_idl: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "gateway": GATEWAY,
            "scenario": scenario,
            "type": DECC_V3_DATA_TYPE,
            "states": {vgeo: 5},
            "version": version,
            "upstream_version": upstream_version,
            "reason": reason,
            "extra": extra,
            "idl": {"type": DECC_V3_IDL_TYPE, "content": ddl, "nested_idl": nested_idl or {}},
            "json_schema": schema_str
        }

    def build_update_payload(self, base_detail: Dict[str, Any], vgeo: str,
                           additions: List[Dict[str, str]],
                           scenario: int,
                           reason: Optional[str] = None,
                           extra_overrides: Optional[Dict[str, Any]] = None,
                           target_version: Optional[int] = None,
                           nested_idl: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """构造更新payload"""
        # 获取基线DDL和Schema（兼容缺失字段的草稿版本）
        base_ddl = (base_detail.get("idl") or {}).get("content", "")
        base_schema_str = base_detail.get("json_schema") or "{}"

        updated_ddl = base_ddl
        updated_schema_str = base_schema_str

        # 版本信息：更新不显式传version，让服务端生成新版本；upstream_version取当前版本
        current_version = base_detail.get("version")
        # 修正：upstream_version 应该优先使用 resolve_target_version 传递进来的正确值（即当前操作的基础版本）
        # 如果是覆盖草稿，则 upstream_version = 当前草稿版本
        # 如果是基于已发布版本新建，则 upstream_version = 已发布版本
        # 但服务端要求 update 时 upstream_version 必须与当前版本一致（或特定逻辑），这里保持与查询详情一致
        upstream_version = base_detail.get("upstream_version") or current_version

        # 针对草稿更新：如果不传 version，服务端会生成新版本号；传了 version 则覆盖。
        # 我们的目标是：如果有草稿，覆盖它；如果没有，基于已应用版本新建。
        # resolve_target_version 已经帮我们选定了 target_version（即最新草稿版本 或 新建版本）。
        # 所以这里我们显式传递 version = target_version 以实现覆盖。
        new_version = target_version
        if new_version is None:
             new_version = (current_version or 0) + 1

        # 处理extra.hdfs.list
        extra = extra_overrides if extra_overrides is not None else base_detail.get("extra", {}) or {}
        direction_pairs = (extra.get("hdfs", {}) or {}).get("list")
        if not direction_pairs:
            direction_pairs = [{"source_vgeo": vgeo, "target_vgeo": "ROW-TT"}]
        # 注入 upstream_version
        extra = {**extra, "hdfs": {"list": direction_pairs}, "upstream_version": upstream_version}

        # 统一使用常量中的 REASON；若调用方提供 reason 则优先使用以保持兼容
        reason_const = reason or REASON

        base_content = self._compose_version_content(vgeo, scenario, new_version, upstream_version,
                                                     reason_const, extra, updated_ddl, updated_schema_str, nested_idl)
        data_overlay = {
            "channel_id": base_detail["channel_id"],
            "data_id": base_detail["data_id"],
            "name": (base_detail.get("name", "").split(".")[-1] if base_detail.get("name") else ""),
            "description": base_detail.get("description", "")
        }
        payload = {
            "gateway": GATEWAY,
            "scenario": scenario,
            "data": {**base_content, **data_overlay},
            "data_version": base_content
        }
        return payload

    def build_create_payload(self, channel_id: str, data_name: str, owners: List[str],
                           vgeo: str, scenario: int, idl_content: str,
                           json_schema_str: str, direction_pairs: List[Dict[str, str]],
                           description: str,
                           reason: Optional[str] = None,
                           extra_data: Optional[Dict[str, Any]] = None,
                           nested_idl: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """构造创建payload"""
        extra = extra_data or {"region": f"{vgeo}-TTP"}
        # 优先使用传入的 reason，否则使用常量
        reason_const = reason or REASON
        return {
            "gateway": GATEWAY,
            # 顶层scenario按列表传递以兼容接口的[]common.Scenario
            "scenario": [scenario],
            "need_submit_data_version": True,
            "data": {
                "channel_id": channel_id,
                "gateway": GATEWAY,
                "scenario": scenario,
                # 仅使用表名，不带库名
                "name": (data_name.split(".")[-1] if data_name else ""),
                "owners": owners,
                "states": {vgeo: 5},
                "extra": extra
            },
            "data_version": {
                "gateway": GATEWAY,
                "scenario": scenario,
                "type": DECC_V3_DATA_TYPE,
                "states": {vgeo: 5},
                "description": description or "",
                "reason": reason_const,
                "extra": {"hdfs": {"list": direction_pairs}},
                "idl": {"type": DECC_V3_IDL_TYPE, "content": idl_content, "nested_idl": nested_idl or {}},
                "json_schema": json_schema_str
            }
        }

    def build_submit_payload(self, data_id: str, version: int, gateway: int, scenario: int) -> Dict[str, Any]:
        """构造提交payload"""
        return {
            "data_id": data_id,
            "version": version,
            "gateway": gateway,
            "scenario": scenario
        }

    # 高阶编排：在现有草稿上更新；若是已应用，则先创建草稿再更新
    def resolve_target_version(self, api: DECCV3API, data_record: Dict[str, Any], vgeo: str,
                               scenario: int, channel_id: str, data_name: str) -> Dict[str, Any]:
        latest_info = (data_record.get("latest_version_states", {}) or {}).get(vgeo, {})
        applied_info = (data_record.get("data_version_states", {}) or {}).get(vgeo, {})
        latest_version = latest_info.get("latestVersion")
        latest_state = latest_info.get("latestVersionState")
        applied_version = applied_info.get("appliedVersion")

        # 强制创建新版本逻辑
        # 不再覆盖 latest_version，而是始终创建新版本
        # upstream_version 取自 applied_version（如果有）或 latest_version（如果只有草稿）
        upstream_version = applied_version if applied_version is not None else latest_version
        if upstream_version is None:
             upstream_version = 0

        # 调用 create_data_version 显式创建一个新草稿版本
        # 这样可以避免列表延迟问题，且保证不覆盖旧草稿
        new_version_resp = api.create_data_version({
            "data_id": data_record["data_id"],
            "gateway": GATEWAY,
            "scenario": scenario,
            "upstream_version": upstream_version,
        })

        # 从响应中获取新创建的版本号
        target_version = new_version_resp.get("version")
        print(f"[DEBUG] create_data_version response ({vgeo}): {json.dumps(new_version_resp, ensure_ascii=False)}") # 添加日志

        if not target_version:
             # 兜底：如果没有返回 version，尝试再次查询或推断
             # 但通常 create_data_version 会返回 version
             # 此时不得不回退到 ref_latest 逻辑，但加上显式延迟查询可能更好
             refreshed = api.get_data_list({
                "gateway": GATEWAY,
                "state": 1,
                "name": data_name,
                "scenario": scenario,
                "channel_id": channel_id,
                "page_number": 1,
                "page_size": 100,
            }).get("data", [])
             ref_latest = (refreshed[0].get("latest_version_states", {}) or {}).get(vgeo, {}) if refreshed else {}
             target_version = ref_latest.get("latestVersion")
             if target_version is None:
                 # 极端保底：如果是首个版本，假设是 1；否则 +1
                 target_version = (latest_version or 0) + 1

        # 拉取目标版本详情作为基线，并返回 upstream_version
        # 注意：如果 target_version 为 0 或 None，可能导致异常，需兜底
        if not target_version:
             # 极端情况：创建了数据但没版本信息，强制设为 1
             target_version = 1

        detail = api.get_data_version_detail({
            "data_id": data_record["data_id"],
            "version": target_version,
            "gateway": GATEWAY,
            "scenario": scenario,
        })
        # 修正 upstream_version 取值逻辑：优先取 detail 中的，其次取 applied，再次取 target-1
        # 对于未发布过的草稿，upstream_version 可能是 0 或 null，此时应设为 current_version (即 target_version)
        upstream_version_detail = detail.get("upstream_version")
        if upstream_version_detail is not None and upstream_version_detail != 0:
             upstream_version = upstream_version_detail
        # 如果 detail 里没拿到（比如新建的），保持 create 时的 upstream_version 即可

        return {
            "target_version": target_version,
            "baseline_detail": detail,
            "upstream_version": upstream_version,
        }


    def _prevalidate_payload_no_chinese(self, payload: Dict[str, Any]) -> None:
        """提交前预校验：若 idl.content 或 json_schema 含中文字符则抛错并打印定位行"""

        def _scan_text(text: str):
            positions = []
            for i, line in enumerate(text.splitlines(), start=1):
                m = re.search(r"[\u4e00-\u9fff]", line)
                if m:
                    col = m.start() + 1
                    snippet = line[max(0, m.start()-20): m.start()+20]
                    positions.append((i, col, snippet))
            return positions

        dv = payload.get("data_version", {})
        data = payload.get("data", {})

        idl_content = None
        json_schema = None

        if isinstance(dv.get("idl"), dict):
            idl_content = dv["idl"].get("content")
        if isinstance(dv.get("json_schema"), str):
            json_schema = dv.get("json_schema")

        # 对 data 层也做兜底（更新载荷会同时携带 data 层内容）
        if not idl_content and isinstance(data.get("idl"), dict):
            idl_content = data["idl"].get("content")
        if not json_schema and isinstance(data.get("json_schema"), str):
            json_schema = data.get("json_schema")

        errors = []
        if isinstance(idl_content, str):
            pos = _scan_text(idl_content)
            if pos:
                errors.append(("idl.content", pos))
        if isinstance(json_schema, str):
            pos = _scan_text(json_schema)
            if pos:
                errors.append(("json_schema", pos))

        if errors:
            # 记录详细定位行并抛错
            for name, pos in errors:
                for (line, col, snippet) in pos:
                    logger.error(f"{name} contains Chinese at line {line}, col {col}: '{snippet}'")
            raise ValueError("Payload contains Chinese characters in idl.content or json_schema; aborting update.")
