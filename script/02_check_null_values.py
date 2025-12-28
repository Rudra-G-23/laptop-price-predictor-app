# ----------------------------------------------------------------
# STEPS:
# 1. Simple Logging 
# 2. User Agent
# 3. Inside the for loop
# 4. check the null values
# 5. Using polars library
# ----------------------------------------------------------------

import polars as pl
from loguru import logger

logger.remove()
logger.add(
    "null_value_log_{time}.log",
    format="{time: YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

try: 
    for i in range(1, 11):
        path = fr"C:\Users\Rudra\Desktop\laptop-price-predictor\data\amazon_laptop_{i}.csv"
        logger.info(f"\n\n\n {path}")
        
        dl = pl.read_csv(path)
        
        #logger.info(f"{dl['os'].to_list()}")
        #logger.info(f"{dl['cpu_model'].to_list()}")
        
        #for col in dl.columns:
        #    logger.info(f"{col}: {dl[col].null_count()}")
            
except Exception as e:
    logger.error(f"     --->>> {e}")