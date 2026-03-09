import os

# Coral
API_HOST = "https://openapi-maliva.byted.org"
# 敏感信息：Coral Auth Key
AUTH_KEY = os.getenv("CORAL_AUTH_KEY", "QTA0MTA4MjBBNTRDQUVEQkY1RTA3MUVDOUFFNjA4OEQ=")

# LLM API配置 - 全局配置
LLM_CONFIG = {
    # 敏感信息：LLM API Key
    "api_key": os.getenv("ARK_API_KEY", "65dc37c8-483e-4e75-8686-739d499bcb1a"),
    "base_url": os.getenv("ARK_BASE_URL", "https://ark-ap-southeast.byteintl.net/api/v3"),
    "model": os.getenv("ARK_MODEL", "ep-20250811104627-r6f86"),
    "max_tokens": 32768,  # 优化：降低token数加快响应
    "temperature": 0.1,   # 优化：适当提高温度减少思考时间,同时降低随机度
    "thinking": {"type": "disabled"},  # 深度思考开关：enabled启用，disabled禁用
}

# Token配置
TOKEN_URL = "https://cloud-i18n.bytedance.net/auth/api/v1/jwt"

# 敏感信息：DECC 服务账号 Secret (用于获取 Token)
# 注意：这里只存 Secret 值，代码中需要自行拼接 "Bearer " 前缀或构造成 JSON
DECC_SERVICE_SECRET = os.getenv("DECC_SERVICE_SECRET", "648fb17dd9052d5183d554762e347fef")
TOKEN_AUTH = f"Bearer {DECC_SERVICE_SECRET}"

# 敏感信息：Coral API 专用服务账号 Secret
CORAL_SERVICE_SECRET = os.getenv("CORAL_SERVICE_SECRET", "73bf90470f882eb418d9d62299de8556")
CORAL_TOKEN_AUTH = f"Bearer {CORAL_SERVICE_SECRET}"
