# =================================================================
# STEPS:
#   - Importing the required libraries, For this we used Polars
#   - 
# =================================================================

import os
import polars as pl
from loguru import logger

# ----------------------------------------------------------------
#                       Logging
# ----------------------------------------------------------------
os.makedirs("logs/", exist_ok=True)

logger.remove()
logger.add(
    "feature_cleaning_log_{time}.log",
    format="{time: YYYY-MM-DD HH:mm:ss} | {level} | {message}" ,
)

# ----------------------------------------------------------------
#                   Configuration
# ----------------------------------------------------------------
STARTING = 1
ENDING = 2

PRICE_COLS= ['offer', 'price']
NEW_PRICE_COLS =['offer_pct', 'price_inr']

DATA_DIR =  r"C:\Users\Rudra\Desktop\laptop-price-predictor\data"
SAVE_DATA_DIR= r"C:\Users\Rudra\Desktop\laptop-price-predictor\clean-data"

# ----------------------------------------------------------------
#               Features Cleaning Functions
# ----------------------------------------------------------------
def clean_price_columns(
    df: pl.DataFrame,
    cols: list[str],
    new_cols: list[str]
) -> pl.DataFrame:
    """
    Clean price-related string columns and create numeric columns.

    Args:
        df (pl.DataFrame): Existing data set
        cols (list[str]): Columns which want to clean
        new_cols (list[str]): New columns name

    Returns:
        pl.DataFrame: Clean column with exiting columns.
    """

    exprs = []
    
    for old_col, new_col in zip(cols, new_cols):
        logger.info(f"Now cleaning column: {old_col}")
        exprs.append(
            pl.col(old_col)
            .cast(pl.Utf8)
            .str.replace_all(r"[₹,%\-]", "")
            .cast(pl.Int16, strict=False)
            .alias(new_col)
        )
        
    return df.with_columns(exprs)
    
# ----------------------------------------------------------------
#                   Dataset Processing
# ----------------------------------------------------------------
def fetch_all_datasets(start: int, end: int) -> None:
    for i in range(start, end + 1):
        try:
            file_path = os.path.join(DATA_DIR, f"amazon_laptop_{i}.csv")
            logger.info(f"Dataset no. {i}")

            df = pl.read_csv(file_path)
            logger.info(f"Dataset shape before cleaning: {df.shape}")
            
            df = clean_price_columns(df, PRICE_COLS, NEW_PRICE_COLS)  
            logger.info(f"Dataset shape after cleaning: {df.shape}")
            
            os.makedirs(SAVE_DATA_DIR, exist_ok=True)
            clean_data_save_path = os.path.join(
                SAVE_DATA_DIR, f"amazon_laptop_{i}_cleaned.csv"
            )
            df.write_csv(clean_data_save_path)

            logger.success(f"Clean dataset {i} saved successfully.\n\n\n")
                
            del df
            
        except Exception as e:
            logger.error(f"     ->>> {e}")
            
        
# ----------------------------------------------------------------
#                        Execution
# ----------------------------------------------------------------
if __name__ == "__main__":
    
    fetch_all_datasets(STARTING, ENDING)
    
    logger.success("\n\n\t Data Cleaning Successfully executed")