#Coral
API_HOST = "https://openapi-maliva.byted.org"
AUTH_KEY = "QTA0MTA4MjBBNTRDQUVEQkY1RTA3MUVDOUFFNjA4OEQ="

# LLM API配置 - 全局配置
LLM_CONFIG = {
    "api_key": "65dc37c8-483e-4e75-8686-739d499bcb1a",
    "base_url": "https://ark-ap-southeast.byteintl.net/api/v3",
    "model": "ep-20250811104627-r6f86",
    "max_tokens": 32768,  # 优化：降低token数加快响应
    "temperature": 0.1,   # 优化：适当提高温度减少思考时间,同时降低随机度
    "thinking": {"type": "disabled"},  # 深度思考开关：enabled启用，disabled禁用
}

# Token配置
TOKEN_URL = "https://cloud-i18n.bytedance.net/auth/api/v1/jwt"
TOKEN_AUTH = "Bearer 648fb17dd9052d5183d554762e347fef"#服务账号jwt

# Coral API 专用 Token 配置
CORAL_TOKEN_AUTH = "Bearer 73bf90470f882eb418d9d62299de8556"
