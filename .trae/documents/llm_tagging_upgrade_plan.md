# 基于 LLM 的智能打标升级计划

## 目标
将现有的基于关键词匹配的字段打标逻辑（`6.1` vs `7`）升级为基于 LLM 的语义理解打标。LLM 将参考 `tag_mapping.md` 文档，为每个字段推荐最合适的 Tag ID。

## 步骤详解

### 1. 升级 TagManager
修改 `decc_automation/tagging/tag_manager.py`，使其支持缓存外部注入的标签映射。

- [ ] 添加 `preloaded_tags` 字典属性。
- [ ] 添加 `preload_tags(tags_map)` 方法。
- [ ] 修改 `get_field_tag` 方法：优先查找 `preloaded_tags`，命中则直接返回；未命中则回退到原有的关键词匹配逻辑。

### 2. 扩展 LLM DDL Processor
修改 `decc_automation/llm/ddl_processor.py`，增加批量推荐标签的能力。

- [ ] 读取 `decc_automation/config/tag_mapping.md` 内容。
- [ ] 实现 `recommend_field_tags(fields)` 方法。
- [ ] 构造 Prompt：包含 Tag Mapping 文档内容和字段列表。
- [ ] 解析 LLM 返回的 JSON 结果。

### 3. 适配 JSONSchemaBuilder
修改 `decc_automation/processors/json_schema_builder.py`，允许注入 TagManager 实例。

- [ ] 修改 `__init__` 方法，接受可选的 `tag_manager` 参数。
- [ ] 如果传入了 `tag_manager`，则使用该实例（从而复用已预加载的标签）；否则新建实例。

### 4. 集成到主流程
修改 `decc_automation/main/decc_flow.py`，串联整个流程。

- [ ] 在 `orchestrate_upsert_by_region` 方法中：
    - [ ] 在获取到 `ddl_info` 后，提取字段列表。
    - [ ] 调用 `self.llm.recommend_field_tags` 获取标签映射。
    - [ ] 实例化 `TagManager` 并调用 `preload_tags`。
    - [ ] 将该 `TagManager` 实例传递给 `JSONSchemaBuilder`。

## 验证
- [ ] 运行测试脚本，验证生成的 Payload 中 `json_schema` 里的 `tag` 字段是否包含 LLM 推荐的 ID（如 `4.1.3`），而不仅仅是 `6.1` 或 `7`。
