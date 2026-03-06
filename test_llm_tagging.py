import sys
import os
import logging
import json

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from decc_automation.llm.ddl_processor import LLMDDLProcessor, DDLInfo, ColumnInfo

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_recommend_field_tags():
    logger.info("Initializing LLM Processor...")
    try:
        processor = LLMDDLProcessor()
    except Exception as e:
        logger.error(f"Failed to initialize processor: {e}")
        return

    # Create mock columns
    columns = [
        ColumnInfo(name="user_id", type="STRING", description="Unique identifier for user", chinese_description="用户ID", original_comment="用户ID"),
        ColumnInfo(name="email_address", type="STRING", description="User email address", chinese_description="用户邮箱", original_comment="用户邮箱"),
        ColumnInfo(name="device_imei", type="STRING", description="Mobile device IMEI", chinese_description="设备IMEI", original_comment="设备IMEI"),
        ColumnInfo(name="ip_address", type="STRING", description="User IP address", chinese_description="IP地址", original_comment="IP地址"),
        ColumnInfo(name="created_at", type="STRING", description="Record creation timestamp", chinese_description="创建时间", original_comment="创建时间"),
        ColumnInfo(name="total_spend", type="DOUBLE", description="Total spending amount", chinese_description="总消费", original_comment="总消费"),
        ColumnInfo(name="country_code", type="STRING", description="Country code", chinese_description="国家代码", original_comment="国家代码"),
        ColumnInfo(name="phone_number", type="STRING", description="User phone number", chinese_description="手机号", original_comment="手机号"),
    ]

    # Create mock DDLInfo
    ddl_info = DDLInfo(
        table_name="test_user_activity",
        database="test_db",
        description="Table containing user activity logs",
        columns=columns,
        partition_columns=[],
        region="SG"
    )

    logger.info("Starting tag recommendation...")
    tags = processor.recommend_field_tags(ddl_info)

    logger.info("=== Tag Recommendation Results ===")
    print(json.dumps(tags, indent=2))

    # Verification
    expected_tags = {
        "user_id": ["4.1.2", "4.1"], # Account property
        "country_code": ["4.1.11"], # country
        "total_spend": ["4.2.4", "4.2.14", "4.2.8"], # Aggregated data / Billing
        "created_at": ["2"], # System/Default
    }

    for field, expected in expected_tags.items():
        actual = tags.get(field)
        if actual not in expected:
            logger.warning(f"Field '{field}': expected one of {expected}, got '{actual}'")
        else:
            logger.info(f"Field '{field}': verified tag '{actual}'")

if __name__ == "__main__":
    test_recommend_field_tags()
