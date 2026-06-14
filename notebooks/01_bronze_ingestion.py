#%pip install entsoe-py

from entsoe import EntsoePandasClient
import pandas as pd
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import time

API_KEY = "f478b7d1-9787-4b54-9e21-bf6b96570fb5"
client  = EntsoePandasClient(api_key=API_KEY)


def fetch_one_month(year, month):
    start = pd.Timestamp(f"{year}-{month:02d}-01", tz="UTC")
    if month == 12:
        end = pd.Timestamp(f"{year+1}-01-01", tz="UTC")
    else:
        end = pd.Timestamp(f"{year}-{month+1:02d}-01", tz="UTC")

    df_gen = client.query_generation("IE", start=start, end=end)
    df_gen.columns = ['_'.join(col).strip() for col in df_gen.columns]

    df_load = client.query_load("IE", start=start, end=end)
    df_load.columns = ["demand_mw"]

    df = df_gen.join(df_load, how="inner")
    df = df.reset_index().rename(columns={"index": "timestamp"})
    return df


def flatten_and_map(df):
    rename_map = {
        "timestamp":                                          "timestamp",
        "Wind Onshore_Actual Aggregated":                    "wind_mw",
        "Fossil Gas_Actual Aggregated":                      "gas_mw",
        "Fossil Hard coal_Actual Aggregated":                "coal_mw",
        "Fossil Oil_Actual Aggregated":                      "oil_mw",
        "Fossil Peat_Actual Aggregated":                     "peat_mw",
        "Hydro Pumped Storage_Actual Aggregated":            "hydro_pumped_mw",
        "Hydro Run-of-river and poundage_Actual Aggregated": "hydro_river_mw",
        "Other_Actual Aggregated":                           "other_mw",
        "Solar_Actual Aggregated":                           "solar_mw",
        "demand_mw":                                         "demand_mw",
    }

    df = df[[c for c in rename_map.keys() if c in df.columns]]
    df = df.rename(columns=rename_map)
    df["country_code"] = "IE"
    df["ingested_at"]  = datetime.now(timezone.utc)
    return df


def backfill_bronze():
    start_date = datetime(2022, 1, 1)
    end_date   = datetime.now(timezone.utc).replace(tzinfo=None)

    current = start_date
    success = []
    failed  = []

    while current < end_date:
        year  = current.year
        month = current.month
        print(f"Fetching {year}-{month:02d}...")

        for attempt in range(3):
            try:
                df_raw    = fetch_one_month(year, month)
                df_mapped = flatten_and_map(df_raw)
                spark_df  = spark.createDataFrame(df_mapped)

                spark_df.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable("eirgrid_dev.bronze.grid_raw")

                success.append(f"{year}-{month:02d}")
                print(f"  Done — {len(df_mapped)} rows")
                break

            except Exception as e:
                print(f"  Attempt {attempt+1} failed — {e}")
                if attempt < 2:
                    time.sleep(5)
                else:
                    failed.append(f"{year}-{month:02d}: {e}")
                    print(f"  Giving up on {year}-{month:02d}")

        time.sleep(1)
        current += relativedelta(months=1)

    print(f"\nComplete. {len(success)} months loaded, {len(failed)} failed.")
    if failed:
        print("Failed months:")
        for f in failed:
            print(f"  {f}")

    return failed


failed_months = backfill_bronze()

# Incremental load fuction to add new data rows in pipeline
def get_last_loaded_timestamp():
    result = spark.sql("""
        SELECT MAX(timestamp) as max_ts 
        FROM eirgrid_dev.bronze.grid_raw
    """).collect()[0]["max_ts"]
    
    return result.replace(tzinfo=None) if result else datetime(2022, 1, 1)


def incremental_load():
    last_loaded = get_last_loaded_timestamp()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    
    print(f"Last loaded: {last_loaded}")
    print(f"Loading up to: {now}")
    
    current = datetime(last_loaded.year, last_loaded.month, 1)
    
    while current <= now:
        df_raw    = fetch_one_month(current.year, current.month)
        df_mapped = flatten_and_map(df_raw)
        
        df_new = df_mapped[df_mapped["timestamp"] > pd.Timestamp(last_loaded, tz="UTC")]
        
        if len(df_new) > 0:
            spark_df = spark.createDataFrame(df_new)
            spark_df.write.format("delta").mode("append") \
                .saveAsTable("eirgrid_dev.bronze.grid_raw")
            print(f"{current.year}-{current.month:02d}: {len(df_new)} new rows")
        else:
            print(f"{current.year}-{current.month:02d}: no new rows")

        current += relativedelta(months=1)
        time.sleep(1)

incremental_load()
