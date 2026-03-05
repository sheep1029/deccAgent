import logging
import re
from typing import Dict, List, Any, Optional

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
        upstream_version = base_detail.get("upstream_version") or current_version
        new_version = target_version if target_version is not None else (current_version or 0) + 1

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
                           extra_data: Optional[Dict[str, Any]] = None,
                           nested_idl: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """构造创建payload"""
        extra = extra_data or {"region": f"{vgeo}-TTP"}
        # 统一使用常量中的 REASON
        reason_const = REASON
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

        # 若已有草稿，直接在该草稿版本上更新
        if latest_state == "draft" and latest_version is not None:
            target_version = latest_version
        else:
            upstream_version = applied_version if applied_version is not None else latest_version
            api.create_data_version({
                "data_id": data_record["data_id"],
                "gateway": GATEWAY,
                "scenario": scenario,
                "upstream_version": upstream_version,
            })
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
                target_version = ((applied_version or latest_version or 0) + 1)

        # 拉取目标版本详情作为基线，并返回 upstream_version
        detail = api.get_data_version_detail({
            "data_id": data_record["data_id"],
            "version": target_version,
            "gateway": GATEWAY,
            "scenario": scenario,
        })
        upstream_version = detail.get("upstream_version") or applied_version or latest_version or target_version - 1

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
