"""
标签管理模块
处理字段的标签逻辑，包括合作数据标签和工程数据标签
"""

import logging
from typing import List, Dict, Optional

from decc_automation.config.constants import COPERATION_LIST, TAG_COPERATION, TAG_ENGINEERING

logger = logging.getLogger(__name__)

class TagManager:
    """标签管理器，处理字段标签的分配（支持LLM预加载和单列表模糊匹配）"""
    
    def __init__(self):
        # 统一使用单列表进行模糊匹配
        self.coperation_keywords = [k.lower() for k in COPERATION_LIST]
        self.tag_coperation = TAG_COPERATION
        self.tag_engineering = TAG_ENGINEERING
        # 预加载的标签映射 {field_name: tag_id}
        self.preloaded_tags: Dict[str, str] = {}
    
    def preload_tags(self, tags_map: Dict[str, str]) -> None:
        """
        预加载字段标签映射（通常来自LLM推荐）
        
        Args:
            tags_map: 字段名到标签ID的映射
        """
        if tags_map:
            self.preloaded_tags.update(tags_map)
            logger.info(f"已预加载 {len(tags_map)} 个字段的标签映射")

    def get_field_tag(self, field_name: str) -> str:
        """
        根据字段名确定标签类型。
        优先查找预加载的映射（LLM结果），未命中则回退到关键词模糊匹配。
        
        Args:
            field_name: 字段名称
            
        Returns:
            str: 标签ID
        """
        # 1. 优先查找预加载映射 (精确匹配)
        if field_name in self.preloaded_tags:
            tag = self.preloaded_tags[field_name]
            # 简单验证 tag 是否有效，若为空则继续走兜底
            if tag and str(tag).strip():
                logger.debug(f"字段 {field_name} 使用预加载标签: {tag}")
                return str(tag)

        # 2. 兜底逻辑：单列表模糊匹配
        field_name_lower = field_name.lower()
        if any(keyword in field_name_lower for keyword in self.coperation_keywords):
            logger.debug(f"字段 {field_name} 匹配到合作数据关键词")
            return self.tag_coperation
        
        # 3. 默认返回工程数据标签
        logger.debug(f"字段 {field_name} 标记为工程数据")
        return self.tag_engineering
    
    def add_coperation_keyword(self, keyword: str, exact_match: bool = False) -> None:
        """
        添加新的合作数据关键词（单列表，忽略精确/模糊区分）
        
        Args:
            keyword: 关键词
            exact_match: 兼容旧签名，不再区分
        """
        keyword = keyword.strip().lower()
        if keyword not in self.coperation_keywords:
            self.coperation_keywords.append(keyword)
            logger.info(f"添加关键词: {keyword}")
    
    def remove_coperation_keyword(self, keyword: str, exact_match: bool = False) -> None:
        """
        移除合作数据关键词（单列表，忽略精确/模糊区分）
        
        Args:
            keyword: 关键词
            exact_match: 兼容旧签名，不再区分
        """
        keyword = keyword.strip().lower()
        self.coperation_keywords = [k for k in self.coperation_keywords if k != keyword]
        logger.info(f"移除关键词: {keyword}")
    
    def get_all_keywords(self) -> dict:
        """
        获取所有关键词列表（单列表）
        
        Returns:
            dict: 兼容旧结构，返回单列表和空精确列表
        """
        return {
            "fuzzy_match": self.coperation_keywords,
            "exact_match": []
        }
    
    def is_coperation_field(self, field_name: str) -> bool:
        """
        判断字段是否为合作数据
        
        Args:
            field_name: 字段名称
            
        Returns:
            bool: True表示是合作数据，False表示是工程数据
        """
        return self.get_field_tag(field_name) == self.tag_coperation
