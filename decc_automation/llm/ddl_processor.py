# LLM DDL处理器：解析DDL、生成英文描述、组装英文化DDL与Schema

import os
import json
import logging
import re
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from openai import OpenAI
from decc_automation.config.constants import *
from decc_automation.config.llm_config import LLMConfigManager
from decc_automation.tagging import TagManager

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """字段信息数据结构"""
    name: str
    type: str
    description: str
    chinese_description: str
    is_primary_key: bool = False
    is_nullable: bool = True
    original_comment: str = ""


@dataclass
class DDLInfo:
    """DDL信息数据结构"""
    table_name: str
    database: str
    description: str
    columns: List[ColumnInfo]
    region: str = ""
    partition_columns: List[ColumnInfo] = None
    # 一次性LLM生成的字段描述映射（包含map子键：field.key）
    field_desc_map: Dict[str, str] = None


@dataclass
class ParsedDDL:
    """解析后的DDL结构"""
    table_name: str
    database: str
    region: str
    columns: List[Dict[str, Any]]
    partition_columns: List[Dict[str, Any]] = None
    original_ddl: str = ""


class LLMDDLProcessor:

    def __init__(self, api_key: str = None, base_url: str = None, model: str = None):
        """
        初始化LLM DDL处理器

        Args:
            api_key: Ark API密钥
            base_url: API基础URL
            model: 模型端点ID
        """
        self.api_key = api_key or LLMConfigManager.get_api_key() or os.getenv("ARK_API_KEY")
        self.base_url = base_url or LLMConfigManager.get_base_url()
        self.model = model or LLMConfigManager.get_model()

        if not self.api_key:
            raise ValueError("LLM API key未配置")

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )

        self.max_tokens = LLMConfigManager.get_max_tokens()
        self.temperature = LLMConfigManager.get_temperature()
        self.thinking = LLMConfigManager.get_thinking()

    def _find_matching_paren(self, text: str, start_index: int = 0) -> int:
        """从指定的起始索引开始，找到匹配的右括号，正确处理SQL字符串和注释。"""
        if text[start_index] != '(':
            raise ValueError("起始索引处没有左括号")

        depth = 1
        in_single_quote = False
        in_double_quote = False

        i = start_index + 1
        while i < len(text):
            char = text[i]

            if char == "'" and (i == 0 or text[i-1] != '\\'):
                in_single_quote = not in_single_quote
            elif char == '"' and (i == 0 or text[i-1] != '\\'):
                in_double_quote = not in_double_quote

            if not in_single_quote and not in_double_quote:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                    if depth == 0:
                        return i
            i += 1

        return -1

    def _parse_ddl_structure(self, ddl: str, table_info: Dict[str, str]) -> ParsedDDL:
        """使用更健壮的方式精确解析DDL结构，支持复杂类型和多个分区字段。"""
        logger.info("开始解析DDL结构...")

        table_pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:`([^`]+)`\.)?`([^`]+)`|([^\s\.]+)(?:\.([^\s\(\)]+))?)\s*\('
        table_name_match = re.search(table_pattern, ddl, re.IGNORECASE)
        if not table_name_match:
            raise ValueError("无法解析表名")

        groups = table_name_match.groups()
        if groups[0] and groups[1]:
            database = groups[0]
            table_name = groups[1]
        elif groups[2] and groups[3]:
            database = groups[2]
            table_name = groups[3]
        elif groups[2]:
            table_name = groups[2]
            database = table_info.get('database', '')
        else:
            raise ValueError("无法解析表名格式")

        open_paren_index = table_name_match.end() - 1
        close_paren_index = self._find_matching_paren(ddl, open_paren_index)
        if close_paren_index == -1:
            raise ValueError("无法找到字段定义的匹配')'")

        fields_section = ddl[open_paren_index + 1 : close_paren_index]

        partition_section = ""
        remaining_ddl = ddl[close_paren_index:]
        partition_match = re.search(r'\s+PARTITIONED\s+BY\s*\(', remaining_ddl, re.IGNORECASE)

        if partition_match:
            part_open_paren_index = close_paren_index + partition_match.end() - 1
            part_close_paren_index = self._find_matching_paren(ddl, part_open_paren_index)

            if part_close_paren_index != -1:
                partition_section = ddl[part_open_paren_index + 1 : part_close_paren_index]

        columns = self._parse_columns(fields_section)
        partition_columns = self._parse_columns(partition_section) if partition_section else []

        return ParsedDDL(
            table_name=table_name,
            database=database,
            region=table_info.get('region', ''),
            columns=columns,
            partition_columns=partition_columns,
            original_ddl=ddl
        )

    def _parse_columns(self, column_section: str) -> List[Dict[str, Any]]:
        """解析字段定义"""
        columns = []
        field_defs = self._split_fields(column_section)

        # 正则表达式解析单个字段定义: `name` type COMMENT 'comment'
        # 修正: 允许字段名以数字开头
        field_regex = re.compile(
            r"^\s*`?([a-zA-Z0-9_]+)`?\s+(.+?)(?:\s+COMMENT\s+['\"](.*?)['\"])?\s*$",
            re.IGNORECASE | re.DOTALL
        )

        for field_def in field_defs:
            field_def = field_def.strip()
            if not field_def:
                continue

            match = field_regex.match(field_def)
            if match:
                name, field_type, comment = match.groups()
                columns.append({
                    'name': name.strip(),
                    'type': field_type.strip(),
                    'comment': (comment or "").strip()
                })
            elif field_def: # 记录无法匹配的字段以供调试
                logger.warning(f"无法使用正则表达式解析字段定义: '{field_def}'")

        return columns

    def _split_fields(self, column_section: str) -> List[str]:
        """智能分割字段"""
        fields = []
        current_field = ""
        paren_depth = 0
        angle_depth = 0
        in_single_quote = False
        in_double_quote = False

        i = 0
        while i < len(column_section):
            char = column_section[i]

            if char == "'" and (i == 0 or column_section[i-1] != '\\'):
                in_single_quote = not in_single_quote
            elif char == '"' and (i == 0 or column_section[i-1] != '\\'):
                in_double_quote = not in_double_quote

            if not in_single_quote and not in_double_quote:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == '<':
                    angle_depth += 1
                elif char == '>':
                    angle_depth -= 1
                elif char == ',' and paren_depth == 0 and angle_depth == 0:
                    if current_field.strip():
                        fields.append(current_field.strip())
                    current_field = ""
                    i += 1
                    continue

            current_field += char
            i += 1

        if current_field.strip():
            fields.append(current_field.strip())

        return fields
    # 第二步：LLM生成描述（内容创作）
    def _generate_descriptions_with_llm(self, parsed_ddl: ParsedDDL, nested_map_defs: Dict[str, Dict[str, Any]] = None) -> Dict[str, Any]:
        """使用LLM一次性生成表描述和所有字段描述"""
        logger.info("开始LLM生成表和字段描述...")

        all_fields = self._collect_all_fields(parsed_ddl, nested_map_defs)

        prompt = f"""
基于以下数据库表信息，生成详细的表描述和所有字段的英文描述。

表名: {parsed_ddl.table_name}
数据库: {parsed_ddl.database}
字段信息:
{json.dumps(all_fields, ensure_ascii=False, indent=2)}

要求：
1. 首先根据表名和字段含义推断表的业务用途，生成具体、专业的英文表描述（不少于50个字符）
2. 为每个字段生成详细的英文描述（不少于20个字符），基于中文注释进行准确翻译和扩展
3. 如果字段的中文注释很简单（如"field date"、"日期字段"等），要根据字段名和类型推断其具体业务含义，生成更有意义的描述
4. 如果字段没有中文注释，根据字段名和类型推断其含义
5. 输出格式为JSON，包含"table_description"和"field_descriptions"两个键
6. field_descriptions是一个对象，键为字段名，值为英文描述
7. 只返回JSON，不要添加其他解释
8. **非常重要**: 所有生成的描述都必须是纯英文，绝对不能包含任何中文字符。

示例输出格式：
```json
{{
  "table_description": "Detailed description of what this table contains and its business purpose",
  "field_descriptions": {{
    "field1": "Detailed English description for field1",
    "field2": "Detailed English description for field2"
  }}
}}
```
"""

        try:
            request_params = {
                "model": self.model,
                "messages": [
                    {
                        "role": "system",
                        "content": "你是专业的数据架构师，擅长根据数据库表结构推断业务含义并生成准确的表和字段描述。"
                    },
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": self.max_tokens,
                "temperature": self.temperature,
            }

            if isinstance(self.thinking, dict):
                request_params["extra_body"] = {"thinking": self.thinking}

            response = self.client.chat.completions.create(**request_params)
            result_text = response.choices[0].message.content.strip()

            # 解析响应
            result = self._parse_combined_description_response(result_text)

            # 确保所有字段都有描述（包含map子键）
            all_field_names = {f['name'] for f in all_fields}
            field_descriptions = result.get('field_descriptions', {})
            # 仅保留 LLM 返回的字段描述，不再填充缺失项
            # 严格校验：若缺失任意字段描述或表描述为空，则截断流程
            missing_fields = []
            for name in all_field_names:
                if name not in field_descriptions or not str(field_descriptions.get(name) or "").strip():
                    missing_fields.append(name)
            table_desc = str(result.get('table_description', '') or '').strip()
            if missing_fields or not table_desc:
                raise ValueError(
                    f"LLM未返回完整描述，缺失字段: {', '.join(missing_fields) if missing_fields else '无'}；表描述为空"
                )

            return {
                'table_description': table_desc,
                'field_descriptions': field_descriptions
            }

        except Exception as e:
            logger.error(f"LLM生成描述失败: {e}")
            # 截断流程：抛错以终止主流程
            raise

    def _parse_combined_description_response(self, response_text: str) -> Dict[str, Any]:
        """解析LLM的表和字段描述组合响应"""
        try:
            # 清理markdown标记
            if "```json" in response_text:
                start = response_text.find("```json") + 7
                end = response_text.find("```", start)
                if end != -1:
                    response_text = response_text[start:end]
            elif "```" in response_text:
                start = response_text.find("```") + 3
                end = response_text.find("```", start)
                if end != -1:
                    response_text = response_text[start:end]

            response_text = response_text.strip()
            return json.loads(response_text)

        except json.JSONDecodeError as e:
            logger.error(f"解析LLM组合描述响应失败: {e}")
            return {}

    def _clean_and_verify_description(self, description: str) -> str:
        """
        最终清理和验证描述：
        1. 移除所有中文字符，作为LLM未完全遵守指令的最后防线。
        2. 移除DECC规范禁止的特殊符号。
        """
        # 1. 移除中文字符
        text_no_chinese = re.sub(r'[\u4e00-\u9fa5]', '', description)

        # 2. 移除特殊符号
        special_chars = [';', '\'', '"', '[', ']', '{', '}', '|', '\\', '<', '>','.']
        cleaned_text = text_no_chinese
        for char in special_chars:
            cleaned_text = cleaned_text.replace(char, ' ')

        # 3. 清理多余的空格
        return ' '.join(cleaned_text.split())

    # 第三步：代码拼接完整数据（最终组装）
    def _build_complete_ddl_info(self, parsed_ddl: ParsedDDL, llm_result: Dict[str, Any]) -> DDLInfo:
        """将解析的结构和LLM生成的描述拼接成完整的DDLInfo"""
        logger.info("开始拼接完整DDL信息...")

        # 提取表描述和字段描述
        raw_table_description = llm_result.get('table_description', "")
        field_descriptions = llm_result.get('field_descriptions', {})

        # 清理和验证表描述
        table_description = self._clean_and_verify_description(raw_table_description)

        # US 环境下对合作数据追加说明（用于英文DDL字段COMMENT保持与JSON Schema一致）
        tag_manager = TagManager()
        region_upper = parsed_ddl.region.upper() if isinstance(parsed_ddl.region, str) else ''
        is_us_or_eu = region_upper.startswith('US') or region_upper.startswith('EU')
        suffix = COOP_SUFFIX_EN

        # 构建字段信息
        columns = [
            self._build_column_info_for(col, field_descriptions, tag_manager, is_us_or_eu, suffix)
            for col in parsed_ddl.columns
        ]

        # 构建分区字段信息
        partition_columns = [
            self._build_column_info_for(col, field_descriptions, tag_manager, is_us_or_eu, suffix)
            for col in parsed_ddl.partition_columns
        ]

        # 保存字段描述映射（包含map子键）
        field_desc_map = {k: self._clean_and_verify_description(v) for k, v in field_descriptions.items()}

        return DDLInfo(
            table_name=parsed_ddl.table_name,
            database=parsed_ddl.database,
            description=table_description,
            columns=columns,
            region=parsed_ddl.region,
            partition_columns=partition_columns,
            field_desc_map=field_desc_map
        )


    def process_ddl(self, ddl: str, table_info: Dict[str, str], nested_ddl_info: Dict[str, Dict[str, str]] = None, nested_map_defs: Dict[str, Dict[str, Any]] = None) -> DDLInfo:
        """

        Args:
            ddl: DDL字符串
            table_info: 表信息字典
            nested_ddl_info: MAP字段信息

        Returns:
            DDLInfo: 处理后的DDL信息
        """
        if not ddl or not ddl.strip():
            raise ValueError("DDL不能为空")

        try:
            logger.info("开始DDL处理流程...")

            # 第一步：代码解析DDL
            parsed_ddl = self._parse_ddl_structure(ddl, table_info)
            logger.info(f"解析完成：表{parsed_ddl.table_name}，共{len(parsed_ddl.columns)}个字段")

            # 第二步：LLM生成描述
            llm_result = self._generate_descriptions_with_llm(parsed_ddl, nested_map_defs)
            logger.info("LLM生成表和字段描述完成")

            # 第三步：代码拼接完整数据
            ddl_info = self._build_complete_ddl_info(parsed_ddl, llm_result)
            logger.info("DDL信息拼接完成")

            return ddl_info

        except Exception as e:
            logger.error(f"DDL处理失败: {e}")
            raise

    def _generate_english_ddl(self, ddl_info: DDLInfo, original_ddl: str) -> str:
        result = original_ddl
        field_desc_map = ddl_info.field_desc_map or {}

        for field_name, new_description in field_desc_map.items():
            pattern = rf'(\s*)(`?){re.escape(field_name)}(`?)\s+([\w<>\s,\(\)"\'\-\.:]+?(?:<(?:[^<>]|(?:<[^<>]*>))*>)?)\s+COMMENT\s+[\'\"]([^\'\"]*)[\'\"]([^\n]*)'

            def replace_single_field(match):
                indent = match.group(1)
                backtick_start = match.group(2)
                backtick_end = match.group(3)
                field_type = match.group(4)
                original_comment = match.group(5)
                suffix = match.group(6)

                return f"{indent}{backtick_start}{field_name}{backtick_end} {field_type} COMMENT '{new_description}'{suffix}"

            result = re.sub(pattern, replace_single_field, result, flags=re.MULTILINE)

        # 去除可能包含中文的别名等属性，尤其是TBLPROPERTIES中的 alias 项
        result = self._remove_ddl_alias_and_chinese(result)
        return result

    def _read_tag_mapping(self) -> str:
        """读取标签映射文档"""
        try:
            mapping_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'tag_mapping.md')
            with open(mapping_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logger.error(f"读取标签映射文档失败: {e}")
            return ""

    def recommend_field_tags(self, ddl_info: DDLInfo) -> Dict[str, str]:
        """
        使用 LLM 根据 Tag Mapping 推荐字段标签
        """
        logger.info("开始 LLM 字段打标推荐...")

        tag_mapping_content = self._read_tag_mapping()
        if not tag_mapping_content:
            logger.warning("未找到标签映射文档，跳过 LLM 打标推荐")
            return {}

        # 准备字段信息列表
        fields_info = []
        for col in ddl_info.columns:
            fields_info.append({
                "name": col.name,
                "type": col.type,
                "comment": col.original_comment, # 使用原始中文注释辅助判断
                "english_description": col.description # 使用生成的英文描述辅助判断
            })

        # 提示词
        prompt = f"""
你是一个数据治理专家。请根据以下《Tag ID 映射文档》和《字段列表》，为每个字段推荐最合适的 Tag ID。

### Tag ID 映射文档
{tag_mapping_content}

### 字段列表
{json.dumps(fields_info, ensure_ascii=False, indent=2)}

### 要求
1. 仔细阅读 Tag ID 映射文档，理解每个 Tag ID 的定义和适用场景。
2. 分析每个字段的名称、类型、注释和英文描述。
3. **优先匹配**：如果字段含义与文档中某个 Tag ID 的描述高度匹配（如 Country -> 4.1.11, Account -> 4.1.2, Aggregated Metric -> 4.2.x），请推荐该 Tag ID。
4. **默认规则**：如果字段在文档中找不到合适的 Tag ID，或者属于系统字段（如创建时间、日期分区），请推荐 Tag ID "2" (Default Data)。
5. **兜底策略**：不要强行匹配不相关的 Tag。如果不确定，使用 "2"。
6. **输出格式**：返回一个 JSON 对象，键为字段名，值为推荐的 Tag ID（字符串格式）。
7. 只返回 JSON，不要包含 Markdown 标记或其他解释。

### 示例输出
{{
  "user_id": "4.1.3",
  "email": "4.1.1",
  "device_id": "4.2.4",
  "created_at": "2",
  "country": "3"
}}
"""

        try:
            request_params = {
                "model": self.model,
                "messages": [
                    {
                        "role": "system",
                        "content": "你是专业的数据治理专家，负责识别数据敏感分级并打标。"
                    },
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": self.max_tokens,
                "temperature": 0.1, # 低温度以保证稳定性
            }

            if isinstance(self.thinking, dict):
                request_params["extra_body"] = {"thinking": self.thinking}

            response = self.client.chat.completions.create(**request_params)
            result_text = response.choices[0].message.content.strip()

            # 解析 JSON
            result = self._parse_combined_description_response(result_text)

            # 验证结果格式
            validated_result = {}
            for field_name, tag_id in result.items():
                if isinstance(field_name, str) and isinstance(tag_id, (str, int, float)):
                    validated_result[field_name] = str(tag_id)

            logger.info(f"LLM 打标推荐完成，共推荐 {len(validated_result)} 个字段")
            return validated_result

        except Exception as e:
            logger.error(f"LLM 打标推荐失败: {e}")
            return {}

    def _remove_ddl_alias_and_chinese(self, ddl_str: str) -> str:
        """清理DDL中不允许的内容：
        - 删除 TBLPROPERTIES 中的 alias 项（可能包含中文）
        - 删除包含中文字符的属性行（保守处理，仅限属性行）
        - 最终兜底：全局移除任何中文字符，确保完全不含中文
        """
        cleaned = ddl_str
        # 删除 TBLPROPERTIES 区块中的 alias = '...'
        cleaned = re.sub(r"(?i)(,?\s*'alias'\s*=\s*'[^']*')", "", cleaned)
        # 删除 TBLPROPERTIES 中因为去除alias导致的多余逗号或空格
        cleaned = re.sub(r"(?i)(TBLPROPERTIES\s*\(\s*)(,\s*)", r"\1", cleaned)
        # 可选：删除中文字符所在的属性行（避免误报，仅匹配 TBLPROPERTIES 行）
        lines = cleaned.splitlines()
        out_lines = []
        for line in lines:
            if "TBLPROPERTIES" in line or line.strip().startswith("'"):
                if re.search(r"[\u4e00-\u9fff]", line):
                    # 跳过包含中文的属性行
                    continue
            out_lines.append(line)
        cleaned = "\n".join(out_lines)
        # 全局移除任何中文字符，避免LLM描述或残留属性带入中文
        cleaned = re.sub(r"[\u4e00-\u9fff]", "", cleaned)
        return cleaned

    def _collect_all_fields(self, parsed_ddl: ParsedDDL, nested_map_defs: Optional[Dict[str, Dict[str, Any]]] = None) -> List[Dict[str, str]]:
        fields: List[Dict[str, str]] = []
        for col in parsed_ddl.columns:
            fields.append({'name': col['name'], 'type': col['type'], 'chinese_comment': col['comment'] or ''})
        for col in parsed_ddl.partition_columns:
            fields.append({'name': col['name'], 'type': col['type'], 'chinese_comment': col['comment'] or ''})
        if nested_map_defs:
            for field_name, key_defs in nested_map_defs.items():
                if isinstance(key_defs, dict):
                    for key_name, meta in key_defs.items():
                        value_type = (meta.get('type') or 'STRING') if isinstance(meta, dict) else 'STRING'
                        chinese_comment = (meta.get('comment') or '') if isinstance(meta, dict) else ''
                        fields.append({'name': f"{field_name}.{key_name}", 'type': value_type, 'chinese_comment': chinese_comment})
        return fields

    def _build_column_info_for(self, col: Dict[str, Any], field_descriptions: Dict[str, str],
                               tag_manager: TagManager, is_us_or_eu: bool, suffix: str) -> ColumnInfo:
        raw_description = field_descriptions.get(col['name'], '')
        cleaned_description = self._clean_and_verify_description(raw_description)
        if is_us_or_eu:
            tag = tag_manager.get_field_tag(col['name'])
            if str(tag) == str(TAG_COPERATION) and not cleaned_description.endswith(suffix):
                cleaned_description += suffix
        return ColumnInfo(
            name=col['name'],
            type=col['type'],
            description=cleaned_description,
            chinese_description=col['comment'],
            original_comment=col['comment']
        )

    def _extract_map_value_type(self, map_type: str) -> str:
        """提取MAP的value类型"""
        match = re.search(r'MAP<[^,]+,\\s*([^>]+)>', map_type.upper())
        if match:
            return match.group(1).strip()
        return "STRING"



    def build_nested_idl_from_map_defs(self, ddl_info: DDLInfo, map_defs: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
        """根据 MAP_DEFS 生成结构化 nested_idl，使 IDL 与 JSON Schema 保持一致。
        输出为 typed 结构（非示例值），子键的类型与 MAP 的 value 类型一致（如未显式指定）。
        """
        if not map_defs:
            return {}
        objects: List[Dict[str, Any]] = []
        string_fields: List[str] = []

        # 构建列名 -> 列类型映射，便于解析MAP的value类型
        col_type_map = {c.name: c.type for c in (ddl_info.columns or [])}

        for field_name, key_defs in map_defs.items():
            if not isinstance(key_defs, dict):
                continue
            # 解析该MAP字段的value类型
            value_type = col_type_map.get(field_name, "STRING")
            try:
                value_type = self._extract_map_value_type(value_type)
            except Exception:
                # 若无法解析，回退为 STRING
                value_type = "STRING"

            # 生成结构化内容：键集合 + 统一的value类型
            keys_struct: Dict[str, Any] = {}
            for key_name, meta in key_defs.items():
                # 子键类型默认与 MAP 的 value 类型一致；如显式提供，则使用显式类型
                explicit_type = value_type
                if isinstance(meta, dict) and meta.get("type"):
                    explicit_type = str(meta.get("type")).upper()
                # 描述优先使用 LLM 的英文描述，其次使用配置的注释
                raw_desc = None
                if ddl_info.field_desc_map and f"{field_name}.{key_name}" in ddl_info.field_desc_map:
                    raw_desc = ddl_info.field_desc_map[f"{field_name}.{key_name}"]
                elif isinstance(meta, dict):
                    raw_desc = meta.get("comment") or f"Key {key_name} of {field_name}"
                else:
                    raw_desc = f"Key {key_name} of {field_name}"
                cleaned_desc = self._clean_and_verify_description(raw_desc)
                keys_struct[key_name] = {
                    "type": explicit_type,
                    "description": cleaned_desc
                }

            content = {
                "map_field": field_name,
                "value_type": value_type,
                "keys": keys_struct
            }
            map_object = {
                "id": "",
                "field": field_name,
                "type": "object",
                "idl": {
                    "type": "json",
                    "content": json.dumps(content, ensure_ascii=True),
                    "entry": ""
                }
            }
            objects.append(map_object)
            string_fields.append(field_name)

        return {
            "objects": objects,
            "string_fields": string_fields,
            "strings": []
        }
