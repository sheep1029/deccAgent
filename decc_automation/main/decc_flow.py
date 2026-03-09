import json
from typing import Dict, List, Tuple, Any, Optional

from decc_automation.api.decc_v3_api import DECCV3API
from decc_automation.api.coral_api import CoralAPI
from decc_automation.services.data_version_service import DataVersionService
from decc_automation.llm.ddl_processor import LLMDDLProcessor, DDLInfo
from decc_automation.config.constants import (
    SCENARIO_BY_VGEO,
    GATEWAY,
    REGION_INPUT_TO_VGEO,
    REGION_INPUT_TO_REAL,
    REASON,
)


class DECCFlowV3:
    def __init__(self):
        self.api = DECCV3API()
        self.coral = CoralAPI()
        self.service = DataVersionService()
        self.llm = LLMDDLProcessor()

    def _resolve_channel_id(self, channel_name: str, vgeo: str, owner: str,
                            db_index: Optional[int]) -> str:
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
        channels = self.api.get_channel_list(params).get("channels", [])
        if not channels:
            params.pop("owner", None)
            channels = self.api.get_channel_list(params).get("channels", [])
        if not channels:
            params.pop("vgeo", None)
            channels = self.api.get_channel_list(params).get("channels", [])
        if not channels:
            minimal = {"state": 1, "gateway": GATEWAY}
            channels = self.api.get_channel_list(minimal).get("channels", [])
        if not channels:
            raise ValueError(f"Channel not found: {channel_name}")
        try:
            channels_sorted = sorted(channels, key=lambda c: int(c.get("channel_id")))
        except Exception:
            channels_sorted = channels
        if db_index is not None:
            if not (0 <= int(db_index) < len(channels_sorted)):
                raise ValueError(f"Invalid db_index {db_index}, available range: 0..{len(channels_sorted)-1}")
            return channels_sorted[int(db_index)]["channel_id"]
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
        return channels_sorted[0]["channel_id"]

    def orchestrate_upsert_by_region(self, channel_name: str, data_name: str,
                                     region_input: str, target_vgeo: str,
                                     owner: str,
                                     additions: List[Dict] = None,
                                     map_defs: Dict[str, Dict[str, Any]] = None,
                                     db_index: Optional[int] = None) -> Dict:
        """若数据不存在则创建，存在则新增版本（使用英文化IDL与Schema）"""
        # 映射 vgeo / region_real / scenario
        vgeo = REGION_INPUT_TO_VGEO.get(region_input, "ROW-TT")
        region_real = REGION_INPUT_TO_REAL.get(region_input, f"{vgeo}-TTP")
        scenario = SCENARIO_BY_VGEO.get(vgeo, 5)

        channel_id = self._resolve_channel_id(channel_name, vgeo, owner, db_index)

        # 查数据是否存在
        datas = self.api.get_data_list({
            "gateway": GATEWAY,
            "name": data_name.split(".")[-1],  # 仅表名
            "scenario": scenario,
            "channel_id": channel_id,
            "page_number": 1,
            "page_size": 100,
        }).get("data", [])

        # DEBUG: 打印查询到的数据详情，用于排查 EU 版本问题
        if datas:
            print(f"\n[DEBUG] Found data for {data_name} ({vgeo}):")
            print(json.dumps(datas[0], ensure_ascii=False, indent=2))
            latest_state = (datas[0].get("latest_version_states") or {}).get(vgeo, {})
            print(f"[DEBUG] Latest State ({vgeo}): {json.dumps(latest_state, ensure_ascii=False)}")

        if not datas:
            # 真正的“不存在” → 走创建
            payload = self.service.build_create_payload(
                channel_id, data_name, [owner], vgeo, scenario,
                english_ddl, json_schema_str, direction_pairs,
                description=ddl_info.description, reason=ddl_info.reason, extra_data=extra_data,
                nested_idl=nested_idl
            )

        # 拉DDL（VA对齐）并英文化 + 构建JSON Schema
        db, table = self._parse_data_name(data_name)
        region = "US-TTP" if vgeo == "US" else ("EU-TTP" if vgeo == "EU" else vgeo)
        ddl = self.coral.get_table_ddl(region, db, table)
        # 合并新增字段到原DDL，确保仅一次LLM处理覆盖全部字段
        if additions:
            ddl = self._add_extra_columns_to_ddl(ddl, additions)
        table_info = self.coral.get_table_info(region, db, table)
        ddl_info: DDLInfo = self.llm.process_ddl(ddl, table_info=table_info, nested_ddl_info=None, nested_map_defs=map_defs)

        # 使用 LLM 推荐字段标签
        field_tags = self.llm.recommend_field_tags(ddl_info)

        english_ddl = ddl
        try:
            english_ddl = self.llm._generate_english_ddl(ddl_info, ddl)
        except Exception:
            pass
        fields = [{"name": c.name, "type": c.type, "comment": c.description or c.chinese_description or ""}
                  for c in ddl_info.columns]
        from decc_automation.processors.json_schema_builder import JSONSchemaBuilder
        builder = JSONSchemaBuilder()
        json_schema_str = builder.build_from_ddl_fields(
            fields,
            vgeo=vgeo,
            nested_map_defs=map_defs,
            llm_field_desc_map=getattr(ddl_info, 'field_desc_map', None),
            preloaded_tags=field_tags
        )
        nested_idl = self.llm.build_nested_idl_from_map_defs(ddl_info, map_defs)

        default_target = "ROW-TT" if vgeo == "US" else vgeo
        final_target_vgeo = target_vgeo or default_target
        direction_pairs = [{"source_vgeo": vgeo, "target_vgeo": final_target_vgeo}]
        extra_data = {"region": region_real}

        if not datas:
            # 不存在 → 走创建
            payload = self.service.build_create_payload(
                channel_id, data_name, [owner], vgeo, scenario,
                english_ddl, json_schema_str, direction_pairs,
                description=ddl_info.description, reason=ddl_info.reason, extra_data=extra_data,
                nested_idl=nested_idl
            )
            self.service._prevalidate_payload_no_chinese(payload)

            # 打印完整的 Create Payload
            print("\n" + "="*50)
            print(">>> 准备创建数据 (Create Payload) <<<")
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            print("="*50 + "\n")

            create_result = self.api.create_data(payload)

            # 确保 data_id 在顶层 (通常 create_data 返回 {"data": "ID"})
            if not create_result.get("data_id") and isinstance(create_result.get("data"), str):
                create_result["data_id"] = create_result["data"]

            # 注入 version，新建默认为 1
            create_result["version"] = 1

            return create_result

        # 存在 → 走新增版本
        data_record = datas[0]
        # 读取上个“已应用”版本的详情，用于继承 reason/description
        applied_version = (data_record.get("data_version_states", {}) or {}).get(vgeo, {}).get("appliedVersion")
        applied_detail = None
        if applied_version is not None:
            applied_detail = self.api.get_data_version_detail({
                "data_id": data_record["data_id"],
                "version": applied_version,
                "gateway": GATEWAY,
                "scenario": scenario,
            })
        resolved = self.service.resolve_target_version(self.api, data_record, vgeo, scenario, channel_id, data_name.split(".")[-1])
        target_version = resolved["target_version"]
        detail = resolved["baseline_detail"]

        # 构造更新载荷（不再依赖mutator二次添加，改为覆盖IDL/Schema）
        update_payload = self.service.build_update_payload(
            detail, vgeo, additions=[],
            scenario=scenario, extra_overrides=None, target_version=target_version,
            nested_idl=nested_idl
        )
        # 统一设置 reason/description：
        # - 若存在已应用版本，则继承其 reason/description
        # - 若不存在已应用版本，则采用本次 LLM 生成的表描述和 Reason
        if applied_detail:
            applied_reason = applied_detail.get("reason") or REASON
            applied_desc = applied_detail.get("description") or (detail.get("description") or "")
            if isinstance(update_payload.get("data"), dict):
                update_payload["data"]["reason"] = applied_reason
                update_payload["data"]["description"] = applied_desc
            if isinstance(update_payload.get("data_version"), dict):
                update_payload["data_version"]["reason"] = applied_reason
                update_payload["data_version"]["description"] = applied_desc
        else:
            llm_desc = ddl_info.description or (detail.get("description") or "")
            llm_reason = ddl_info.reason or REASON
            if isinstance(update_payload.get("data"), dict):
                update_payload["data"]["reason"] = llm_reason
                update_payload["data"]["description"] = llm_desc
            if isinstance(update_payload.get("data_version"), dict):
                update_payload["data_version"]["reason"] = llm_reason
                update_payload["data_version"]["description"] = llm_desc

        # 覆盖英文化IDL与新Schema
        if isinstance(update_payload.get("data_version", {}), dict):
            update_payload["data_version"]["idl"]["content"] = english_ddl
            update_payload["data_version"]["json_schema"] = json_schema_str
            update_payload["data_version"]["idl"]["nested_idl"] = nested_idl or {}
        if isinstance(update_payload.get("data", {}), dict):
            update_payload["data"]["idl"] = {"type": "hive-ddl", "content": english_ddl, "nested_idl": nested_idl or {}}
            update_payload["data"]["json_schema"] = json_schema_str

        self.service._prevalidate_payload_no_chinese(update_payload)

        # 打印完整的 Update Payload
        print("\n" + "="*50)
        print(">>> 准备提交版本 (Update Payload) <<<")
        print(json.dumps(update_payload, ensure_ascii=False, indent=2))
        print("="*50 + "\n")

        update_result = self.api.update_data_version(update_payload)
        if isinstance(update_result, dict):
            if not update_result.get("data_id"):
                update_result["data_id"] = data_record.get("data_id")
            # 注入 version
            if isinstance(update_payload.get("data_version"), dict):
                update_result["version"] = update_payload["data_version"].get("version")

        return update_result

    def _add_extra_columns_to_ddl(self, ddl: str, extra_columns: List[Dict]) -> str:
        """在DDL中添加新增字段"""
        if not extra_columns:
            return ddl
        # 提取现有列，避免重复
        existing = []
        try:
            start = ddl.find('(')
            end = ddl.rfind(')')
            if start != -1 and end != -1 and start < end:
                col_defs_str = ddl[start+1: end]
                import re
                matches = re.findall(r'\s*`([^`]+)`', col_defs_str)
                if matches:
                    existing = [m.lower() for m in matches]
        except Exception:
            existing = []

        new_cols = [c for c in extra_columns if c.get('name','').lower() not in existing]
        if not new_cols:
            return ddl

        lines = ddl.split('\n')
        insert_index = -1
        for i, line in enumerate(lines):
            if ')' in line and 'PARTITIONED BY' not in line and 'STORED AS' not in line:
                insert_index = i
                break
        if insert_index == -1:
            # 退化为尾部注释
            ddl += "\n\n-- New Column Definitions:\n"
            for col in new_cols:
                comment = col.get('comment') or col.get('description') or ''
                ddl += f"-- {col['name']} {col['type']} COMMENT '{comment}'\n"
            return ddl

        new_lines = lines[:insert_index]
        if insert_index > 0:
            last = new_lines[-1].rstrip()
            if not last.endswith(',') and last.strip() and 'CREATE' not in last:
                new_lines[-1] = last + ','
        for col in new_cols:
            comment = col.get('comment') or col.get('description') or ''
            col_line = f"  `{col['name']}` {col['type']}"
            if comment:
                col_line += f" COMMENT '{comment}'"
            col_line += ","
            new_lines.append(col_line)
        new_lines.extend(lines[insert_index:])
        return '\n'.join(new_lines)


    @staticmethod
    def _parse_data_name(data_name: str) -> Tuple[str, str]:
        parts = data_name.split('.')
        if len(parts) != 2:
            raise ValueError(f"Invalid data name: {data_name}")
        return parts[0], parts[1]
