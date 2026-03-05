"""
标签管理模块
处理字段的标签逻辑，包括合作数据标签和工程数据标签
"""

import logging
from typing import List

from decc_automation.config.constants import COPERATION_LIST, TAG_COPERATION, TAG_ENGINEERING

logger = logging.getLogger(__name__)

class TagManager:
    """标签管理器，处理字段标签的分配（单列表方案）"""
    
    def __init__(self):
        # 统一使用单列表进行模糊匹配
        self.coperation_keywords = [k.lower() for k in COPERATION_LIST]
        self.tag_coperation = TAG_COPERATION
        self.tag_engineering = TAG_ENGINEERING
    
    def get_field_tag(self, field_name: str) -> str:
        """
        根据字段名确定标签类型（单列表模糊匹配）
        
        Args:
            field_name: 字段名称
            
        Returns:
            str: 标签值，合作数据为"7"，工程数据为"6.1"
        """
        field_name_lower = field_name.lower()
        
        # 单列表模糊匹配
        if any(keyword in field_name_lower for keyword in self.coperation_keywords):
            logger.debug(f"字段 {field_name} 匹配到合作数据关键词")
            return self.tag_coperation
        
        # 默认返回工程数据标签
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
