import os
import polars as pl
from loguru import logger

os.makedirs("logs/", exist_ok=True)
logger.remove()
logger.add(
    "feature_cleaning_log_{time}.log",
    format="{time: YYYY-MM-DD HH:mm:ss} | {level} | {message}" ,
)

# CONFIG
STARTING = 1
ENDING = 10

def fetch_all_datasets(STARTING, ENDING):
    for i in range(STARTING, ENDING+1):
        path = fr"C:\Users\Rudra\Desktop\laptop-price-predictor\data\amazon_laptop_{i}.csv"
        logger.info(f" Starting cleaning dataset {i}")
        
        df = pl.read_csv(path)
        del df
        
if __name__ == "__main__":
    
    fetch_all_datasets(STARTING=1, ENDING=10)
    
    logger.info("\n\n\t Data Cleaning Successfully executed")

