from decc_automation.config.settings import LLM_CONFIG
from typing import Dict, Any


class LLMConfigManager:
    """LLM配置管理器"""
    
    @staticmethod
    def get_config() -> Dict[str, Any]:
        """获取LLM配置"""
        return LLM_CONFIG.copy()
    
    @staticmethod
    def get_api_key() -> str:
        """获取API密钥"""
        return LLM_CONFIG["api_key"]
    
    @staticmethod
    def get_base_url() -> str:
        """获取API基础URL"""
        return LLM_CONFIG["base_url"]
    
    @staticmethod
    def get_model() -> str:
        """获取模型ID"""
        return LLM_CONFIG["model"]
    
    @staticmethod
    def get_max_tokens() -> int:
        """获取最大token数"""
        return LLM_CONFIG.get("max_tokens", 4000)
    
    @staticmethod
    def get_temperature() -> float:
        """获取温度参数"""
        return LLM_CONFIG.get("temperature", 0.1)
        
    @staticmethod
    def get_thinking() -> Dict[str, Any]:
        """获取thinking配置"""
        return LLM_CONFIG.get("thinking", {"type": "disabled"})
    
    @staticmethod
    def set_thinking_enabled(enabled: bool = True) -> None:
        """
        设置是否启用深度思考
        
        Args:
            enabled: True启用深度思考，False禁用深度思考
        """
        LLM_CONFIG["thinking"] = {"type": "enabled" if enabled else "disabled"}
    