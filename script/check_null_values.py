# ----------------------------------------------------------------
#                       Simple Logging & User Agent
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
        
        for col in dl.columns:
            logger.info(f"{col}: {dl[col].null_count()}")
            
except Exception as e:
    logger.error(f"     --->>> {e}")