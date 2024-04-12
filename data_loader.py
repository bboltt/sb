from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

def get_latest_data_per_client(df):
    """
    Reduces the data to only include the latest row per client based on the business date.
    
    Args:
        df (DataFrame): Spark DataFrame containing client data with a business date.
    
    Returns:
        DataFrame: DataFrame containing only the latest entry per client.
    """
    windowSpec = Window.partitionBy("hh_id_in_wh").orderBy(col("business_date").desc())
    latest_df = df.withColumn("row_num", row_number().over(windowSpec)).filter(col("row_num") == 1).drop("row_num")
    return latest_df
