# ----------------------------------------------------------------
# STEPS:
# 1. Simple Logging 
# 2. User Agent
# 3. Inside the for loop
# 4. check the null values
# 5. Using polars library
# 6. Fill the null values 
# 7. Strategy with the forward, backward, other filling methods
# ----------------------------------------------------------------

import os
import polars as pl
from loguru import logger

os.makedirs("logs", exist_ok=True)
logger.remove()
logger.add(
    "logs/fill_null_values_log_{time}.log",
    format="{time: YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

def forward_filling(col):
    return dl.with_columns(
        pl.col(col).fill_null(strategy='forward')
    )

    
try: 
    for i in range(1, 11):
        path = fr"C:\Users\Rudra\Desktop\laptop-price-predictor\data\amazon_laptop_{i}.csv"
        logger.info(f"\n\n\n {path}")
        
        dl = pl.read_csv(path)

        forward_fill_cols = ['rating', 'total_rating', 'next_month_bought', 'offer', 'special_features']
        dl = forward_filling(forward_fill_cols)
        logger.info(f""""Forward Fill done with 
                        ['rating', 'total_rating', 'next_month_bought', 'offer', 'special_features']
                    """)
        
        for col in dl.columns:
            logger.info(f"{col}: {dl[col].null_count()}")
            
except Exception as e:
    logger.error(f"     --->>> {e}")