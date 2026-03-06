# DECC v3 API 配置
DECC_V3_CONFIG = {
    "base_url": "https://paas-gw-us.byted.org",
    "headers": {
        'origin': 'https://cloud.bytedance.net',
        'referer': 'https://cloud.bytedance.net/',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'cache-control': 'no-cache',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'domain': 'decc_platform;v1',
    },
    "endpoints": {
        "get_channel_list": "/openapi/channel/list",
        "get_data_list": "/openapi/data/list",
        "get_data_version_detail": "/openapi/data_version/detail",
        "update_data_version": "/openapi/data_version/update",
        "create_data": "/openapi/data/create",
        "submit_data_version": "/openapi/data_version/submit",
        "create_data_version": "/openapi/data_version/create"
    }
}

# Coral API 配置 (用于获取 DDL)
CORAL_CONFIG = {
    "base_url": "https://bc-sg-gw.tiktok-row.net",
    "endpoints": {
        "get_hive_ddl": "/v2/bridge/hive/ddl",
    }
}

# 标签配置
COPERATION_LIST = ['lob', 'owner', 'user', 'department', 'creator', 'approver', 'customer','operator','managers','receivers','department']

# DECC配置信息
REASON = "If this synchronization is not performed, the relevant regions will not have the necessary data or indexes for the modified tables, which will prevent the construction of data and may hinder the optimization of decc work orders and the product experience of the ttp return row computer room, ultimately reducing efficiency."

# Tag映射
TAG_ENGINEERING = "6.1"
TAG_COPERATION = "7"
COOP_SUFFIX_EN = ", it is BDEE/USTS employee information"

# 传参固定配置
GATEWAY = 6   # HDFS
DECC_V3_DATA_TYPE = 3  # 3-Aggregates User Data；2-Default Data
DECC_V3_IDL_TYPE = "hive-ddl"

# vgeo到scenario的映射
SCENARIO_BY_VGEO = {
    "US": 2,
    "EU": 3,
    "CN": 4,
    "ROW-TT": 5,
    "NonTT": 5
}

# 用户输入的region到真实region字符串的映射（用于 data.extra.region 与 CoralAPI 参数）
REGION_INPUT_TO_REAL = {
    "CN": "China-North",
    "EU": "EU-TTP",
    "US": "US-TTP",
    "LARK_SG": "Singapore-SaaS",
    "MY_BD": "AsiaSinf-SouthEast",
    "JP_LARK": "Asia-SaaS",
    "MY_CIS": "Asia-CIS",
    "VA": "US-East",
    "SG": "Singapore-Central",
}

# 用户输入的region到 vgeo 的映射（用于 states 与 hdfs.source_vgeo 以及 scenario）
REGION_INPUT_TO_VGEO = {
    "US": "US",
    "EU": "EU",
    "SG": "ROW-TT",
    "VA": "ROW-TT",
    "CN": "CN",
    "MY_BD": "NonTT",
    "JP_LARK": "NonTT",
    "LARK_SG": "NonTT",
    "MY_CIS": "NonTT",
}

# ddl里的type到decc type的映射
HIVE_TYPE_MAP_DECC_TYPE = {
    'STRING': 'string',
    'String': 'string',
    'string': 'string',
    'BIGINT': 'integer',
    'Bigint': 'integer',
    'bigint': 'integer',
    'INT': 'integer',
    'Int': 'integer',
    'int': 'integer',
    'DOUBLE': 'number',
    'Double': 'number',
    'double': 'number',
    'boolean': 'boolean',
    'Boolean': 'boolean',
    'BOOLEAN': 'boolean'
}
