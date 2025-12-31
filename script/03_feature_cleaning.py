# =================================================================
# STEPS:
#   - Importing the required libraries, For this we used Polars
#   - One by one most of columns clean by one time
#   - Create a separate functions for one type of cleaning
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

PRICE_COLS= ['offer', 'price', 'mrp']
NEW_PRICE_COLS =['offer_pct', 'price_inr', 'mrp_inr']


DATA_DIR =  r"C:\Users\Rudra\Desktop\laptop-price-predictor\data\raw"
SAVE_DATA_DIR= r"C:\Users\Rudra\Desktop\laptop-price-predictor\data\processed"

MODIFIED_COLS: set[str] = set()
NEW_COLS: set[str] = set()

COLUMN_MAPS = {
    "os": {
        "mac": "Mac",
        "window": "Window",
    },
    "colour": {
        "silver|platinum": "Silver",
        "grey|gray|graphite": "Gray",
        "black": "Black",
        "blue": "Blue",
        "white": "White",
    },
    "cpu_model": {
        "Intel|core|Core": "Intel",
        "AMD|ryzen|Ryzen": "AMD",
        "Celeron": "Celeron",
        "Snapdragon": "Snapdragon",
        "MediaTek": "MediaTek",
        "Mac|Apple": "Mac"
    },
}

NEW_MAP_COLS = {
    "os": "os_name",
    "colour": "color_clean",
    "cpu_model": "cpu_brand"
}

# ----------------------------------------------------------------
#               Features Cleaning Functions
# ----------------------------------------------------------------

# New Columns
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
        logger.info(f"Now cleaning column: `{old_col}` --> New col formed: `{new_col}`")
        
        MODIFIED_COLS.add(old_col)
        NEW_COLS.add(new_col)
        
        exprs.append(
            pl.col(old_col)
            .cast(pl.Utf8)
            .str.replace_all(r"[₹,%\-]", "")
            .cast(pl.Int16, strict=False)
            .alias(new_col)
        )
    return df.with_columns(exprs)

# Modified columns
def clean_rating_col(
    df: pl.DataFrame,
) -> pl.DataFrame:
    
    logger.info(f"Now cleaning column: `total_rating`")
    MODIFIED_COLS.add('total_rating')
    
    return df.with_columns(
        pl.col("total_rating")
      .cast(pl.Utf8)
      .str.replace_all(r"[^\d]", "")
      .cast(pl.Int16)
      .alias("total_rating")
    )

def clean_screen_size_col(
    df: pl.DataFrame,
) -> pl.DataFrame:
    
    logger.info(f"Now cleaning column: `screen_size` -> New col formed `screen_size_inches`")
    NEW_COLS.add("screen_size_inches")
    
    return df.with_columns(
            pl.when(pl.col("screen_size").str.contains("Centimetres"))
            .then(
                pl.col("screen_size")
                    .str.split(" ")
                    .list.first()
                    .cast(pl.Float32)
                    * 0.393701
            )
            .otherwise(
                pl.col("screen_size")
                    .str.split(" ")
                    .list.first()
                    .cast(pl.Float32)
            )
            .alias("screen_size_inches")
        )

def clean_hard_disk_col(
    df: pl.DataFrame
) -> pl.DataFrame:
    
    logger.info(f"Now cleaning column: `colour` -> New col formed `color_clean`")
    MODIFIED_COLS.add('hard_disk_size')
    NEW_COLS.add('hdd_in_gb')
    
    return df.with_columns(
    pl.when(pl.col("hard_disk_size").str.contains("TB"))
        .then(
            pl.col("hard_disk_size")
            .str.extract(r"(\d+)")
            .cast(pl.Int16)
            * 1000
            )
        .otherwise(
            pl.col("hard_disk_size")
            .str.extract(r"(\d+)")
            .cast(pl.Int32)
            
            )
        .alias("hdd_in_gb")
    )

def clean_ram_col(
    df:pl.DataFrame
) -> pl.DataFrame:
    
    logger.info(f"Now cleaning column: `ram` -> New col formed `ram_in_gb`")
    MODIFIED_COLS.add('ram')
    NEW_COLS.add('ram_in_gb')
    
    return df.with_columns(
        pl.col('ram')
        .str.extract(r"(\d+)")
        .cast(pl.Int16)
        .alias(r"ram_in_gb")
    )

def clean_map_cols(
    df, column_maps, new_col_map
) -> pl.DataFrame:
    
    exprs = []
    
    for col, patterns in column_maps.items():
        new_col = new_col_map[col]
        
        logger.info(f"Now cleaning column: `{col}` -> New col formed `{new_col}`")
        MODIFIED_COLS.add(col)
        NEW_COLS.add(new_col)   
    
        expr = pl.lit("Other") # default value
        
        for pattern, value in patterns.items():
                expr = (
                    pl.when(
                        pl.col(col).is_not_null()
                        & pl.col(col)
                        .str.to_lowercase()
                        .str.contains(pattern)
                    )
                    .then(pl.lit(value))   
                    .otherwise(expr)
                )
        exprs.append(expr.alias(new_col))
    
    return df.with_columns(exprs)

# ----------------------------------------------------------------
#                   Dataset Processing
# ----------------------------------------------------------------
     
def load_and_clean_dataset(file_path: str) -> pl.DataFrame:
    df = pl.read_csv(file_path)
    logger.info(f"Dataset shape before cleaning: {df.shape}")
    
    df = clean_price_columns(df, PRICE_COLS, NEW_PRICE_COLS)  
    df = clean_rating_col(df)
    df = clean_screen_size_col(df)
    df = clean_hard_disk_col(df)
    df = clean_ram_col(df)
    df = clean_map_cols(df,COLUMN_MAPS, NEW_MAP_COLS)
    
    logger.info(f"Dataset shape after cleaning: {df.shape}")

    return df

def save_clean_dataset(df:pl.DataFrame, i:int) -> None:
    os.makedirs(SAVE_DATA_DIR, exist_ok=True)
    
    clean_data_save_path = os.path.join(
        SAVE_DATA_DIR, f"amazon_laptop_{i}_cleaned.csv"
    )
    
    df.write_csv(clean_data_save_path)

    logger.success(f"Clean dataset {i } saved successfully.\n\n\n")
             
def fetch_all_datasets(start: int, end: int) -> None:
    for i in range(start, end + 1):
        try:
            file_path = os.path.join(DATA_DIR, f"amazon_laptop_{i}.csv")
            logger.info(f"Dataset no. {i}")
            
            df = load_and_clean_dataset(file_path)
            save_clean_dataset(df, i)
            
            del df
            
        except Exception:
            logger.exception(f" Error while processing dataset {i}")
            
        
# ----------------------------------------------------------------
#                        Execution
# ----------------------------------------------------------------
if __name__ == "__main__":
    
    fetch_all_datasets(STARTING, ENDING)
    
    logger.info(f"Modified columns:\n{sorted(MODIFIED_COLS)}\n")
    logger.info(f"New columns created:\n{sorted(NEW_COLS)}\n")
    
    logger.success("\n\n\t Data Cleaning Successfully executed")