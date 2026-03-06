import sys
import os
import logging

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from decc_automation.api.coral_api import CoralAPI

# Setup logging
logging.basicConfig(level=logging.INFO)

def test_get_ddl():
    api = CoralAPI()
    
    # Using the table mentioned by the user previously
    # db_name: ad_dwa
    # table_name: dwa_ole_vo_signal_quantity_spearman_r7d_df_utc0
    
    try:
        ddl = api.get_table_ddl("SG", "ad_dwa", "dwa_ole_vo_signal_quantity_spearman_r7d_df_utc0")
        print("=== DDL Retrieved Successfully ===")
        print(ddl[:500] + "..." if len(ddl) > 500 else ddl)
        print("================================")
    except Exception as e:
        print(f"FAILED to get DDL: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_get_ddl()
