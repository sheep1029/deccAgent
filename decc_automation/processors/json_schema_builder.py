import json
import logging
import re
from typing import Dict, Any, List, Optional
from decc_automation.config.constants import HIVE_TYPE_MAP_DECC_TYPE, TAG_COPERATION, COOP_SUFFIX_EN
from decc_automation.tagging.tag_manager import TagManager

logger = logging.getLogger(__name__)

class JSONSchemaBuilder:
    """JSON Schema构建"""

    def __init__(self) -> None:
        self.tag_manager = TagManager()
        # Ensure compatibility with NCMD schema format

    def _is_account_info_tag(self, tag_id: str) -> bool:
        """
        判断标签是否属于账户信息 (Account Info)
        根据 Tag ID 判断，如果以 '4.1' 开头（维度/属性，包括 Account），则默认为 True
        """
        if not tag_id:
            return False
        # Account property (4.1.2), Advertiser (4.1.6) 等均属于 4.1.x
        return str(tag_id).startswith('4.1')

    def _build_property_def(self, field_name: str, field_type: str, field_comment: str,
                             vgeo: Optional[str] = None,
                             nested_map_defs: Optional[Dict[str, Dict[str, Any]]] = None,
                             llm_field_desc_map: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        tag = self.tag_manager.get_field_tag(field_name)
        json_type = self._map_hive_type_to_json_type(field_type)
        desc = field_comment or f"Field {field_name} of type {field_type}"
        is_us = isinstance(vgeo, str) and vgeo.upper().startswith('US')
        if is_us and str(tag) == str(TAG_COPERATION):
            if not desc.endswith(COOP_SUFFIX_EN):
                desc += COOP_SUFFIX_EN

        prop: Dict[str, Any] = {
            "description": desc,
            "type": json_type,
            "des": {
                "original_type": field_type.lower(),
                "aggr_type": tag,
                "sync": "YES",
                "tpg_account_info_tag": {"is_account_info": self._is_account_info_tag(tag)},
                "ciphered_tag": {}
            }
        }

        if "ARRAY" in field_type.upper():
            item_type = self._extract_array_item_type(field_type)
            mapped_item_type = 'object' if item_type.upper().startswith('STRUCT') else HIVE_TYPE_MAP_DECC_TYPE.get(item_type.upper(), 'string')
            item_desc = prop.get("description") or field_comment or f"Items of {field_name}"
            prop["items"] = {
                "type": mapped_item_type,
                "description": item_desc,
                "des": {
                    "original_type": item_type.lower(),
                    "aggr_type": tag,
                    "sync": "YES"
                }
            }

        if "MAP" in field_type.upper() and nested_map_defs and field_name in nested_map_defs:
            prop["type"] = "object"
            prop["properties"] = {}
            for key_name, meta in nested_map_defs.get(field_name, {}).items():
                value_type = (meta.get('type') or 'STRING') if isinstance(meta, dict) else 'STRING'
                child_json_type = self._map_hive_type_to_json_type(value_type)
                child_desc_key = f"{field_name}.{key_name}"
                if llm_field_desc_map and child_desc_key in llm_field_desc_map:
                    child_desc = llm_field_desc_map[child_desc_key]
                else:
                    child_desc = (meta.get('comment') or f"Key {key_name} of {field_name}") if isinstance(meta, dict) else f"Key {key_name} of {field_name}"
                if is_us and str(tag) == str(TAG_COPERATION):
                    if not child_desc.endswith(COOP_SUFFIX_EN):
                        child_desc += COOP_SUFFIX_EN
                child_def: Dict[str, Any] = {
                    "description": child_desc,
                    "type": child_json_type,
                    "des": {
                        "original_type": value_type.lower(),
                        "aggr_type": tag,
                        "sync": "YES"
                    }
                }
                if "ARRAY" in value_type.upper():
                    item_type = self._extract_array_item_type(value_type)
                    mapped_item_type = 'object' if item_type.upper().startswith('STRUCT') else HIVE_TYPE_MAP_DECC_TYPE.get(item_type.upper(), 'string')
                    child_def["items"] = {
                        "type": mapped_item_type,
                        "des": {
                            "original_type": item_type.lower(),
                            "aggr_type": tag,
                            "sync": "YES"
                        }
                    }
                prop["properties"][key_name] = child_def
        return prop

    def build_from_ddl_fields(self, fields: List[Dict[str, str]], vgeo: Optional[str] = None,
                              nested_map_defs: Optional[Dict[str, Dict[str, Any]]] = None,
                              llm_field_desc_map: Optional[Dict[str, str]] = None,
                              preloaded_tags: Optional[Dict[str, str]] = None) -> str:
        """
        从DDL字段列表构建JSON Schema字符串

        Args:
            fields: 字段列表，每个字段包含name, type, comment等
            preloaded_tags: 预加载的标签字典 {field_name: tag_id}

        Returns:
            JSON Schema字符串
        """
        logger.info(f"从DDL字段构建JSON Schema，字段数: {len(fields)}")

        # 预加载标签
        if preloaded_tags:
            self.tag_manager.preload_tags(preloaded_tags)

        schema = {
            "type": "object",
            "des": {
                "tpg_account_info_tag": {"is_account_info": False},
                "ciphered_tag": {}
            },
            "items": None,
            "properties": {},
            "patternProperties": None
        }

        for field in fields:
            field_name = field.get('name', '')
            field_type = field.get('type', 'STRING')
            field_comment = field.get('comment', '')
            property_def = self._build_property_def(
                field_name, field_type, field_comment,
                vgeo=vgeo,
                nested_map_defs=nested_map_defs,
                llm_field_desc_map=llm_field_desc_map
            )
            schema["properties"][field_name] = property_def

        return json.dumps(schema, ensure_ascii=False, indent=2)

    def update_schema(self, schema_str: str, additions: List[Dict[str, str]], deletions: List[str], vgeo: Optional[str] = None,
                      nested_map_defs: Optional[Dict[str, Dict[str, Any]]] = None,
                      llm_field_desc_map: Optional[Dict[str, str]] = None) -> str:
        """
        更新JSON Schema字符串

        Args:
            schema_str: 原始JSON Schema字符串
            additions: 要添加的字段列表
            deletions: 要删除的字段名列表

        Returns:
            更新后的JSON Schema字符串
        """
        logger.info(f"更新JSON Schema，添加 {len(additions)} 个字段，删除 {len(deletions)} 个字段")

        try:
            schema = json.loads(schema_str)
        except json.JSONDecodeError as e:
            logger.error(f"JSON Schema解析失败: {e}")
            raise ValueError(f"无效的JSON Schema: {e}")

        # 确保基本结构存在
        if "properties" not in schema:
            schema["properties"] = {}

        # 删除字段
        for field_name in deletions:
            if field_name in schema["properties"]:
                del schema["properties"][field_name]
                logger.info(f"删除字段: {field_name}")
            else:
                logger.warning(f"尝试删除不存在的字段: {field_name}")

        for field in additions:
            field_name = field.get('name', '')
            field_type = field.get('type', 'STRING')
            field_comment = field.get('comment', '')
            if field_name in schema["properties"]:
                logger.warning(f"字段已存在，将覆盖: {field_name}")
            property_def = self._build_property_def(
                field_name, field_type, field_comment,
                vgeo=vgeo,
                nested_map_defs=nested_map_defs,
                llm_field_desc_map=llm_field_desc_map
            )
            schema["properties"][field_name] = property_def
            logger.info(f"添加字段: {field_name}")

        return json.dumps(schema, ensure_ascii=False, indent=2)

    @staticmethod
    def _map_hive_type_to_json_type(hive_type: str) -> str:
        """将Hive类型映射到JSON Schema类型"""
        hive_type_upper = hive_type.upper().strip()

        # 处理数组类型
        if "ARRAY" in hive_type_upper:
            return "array"

        # 处理MAP和STRUCT类型
        if "MAP" in hive_type_upper or "STRUCT" in hive_type_upper:
            return "object"

        # 基本类型映射
        for hive_key, json_value in HIVE_TYPE_MAP_DECC_TYPE.items():
            if hive_key.upper() in hive_type_upper:
                return json_value

        # 默认返回string
        logger.warning(f"未找到Hive类型 {hive_type} 的映射，默认使用string")
        return "string"

    @staticmethod
    def _extract_array_item_type(array_type: str) -> str:
        """提取数组元素类型"""
        # 匹配ARRAY<类型>格式
        match = re.search(r'ARRAY\s*<\s*([^>]+)\s*>', array_type, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return "STRING"
