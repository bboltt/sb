from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

def prepare_features_for_clustering(df, input_features):
    """
    Prepare features for clustering by assembling them into a vector and scaling.
    
    Args:
        df (DataFrame): Input DataFrame.
        input_features (list of str): List of column names to be used as features.
    
    Returns:
        DataFrame: DataFrame with a column 'scaledFeatures' containing scaled feature vectors.
    """
    assembler = VectorAssembler(inputCols=input_features, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    
    # Creating a pipeline and fitting it to the features
    pipeline = Pipeline(stages=[assembler, scaler])
    model = pipeline.fit(df)
    scaled_df = model.transform(df)
    
    return scaled_df



from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StringIndexer
from datetime import datetime

class FeatureEngineeringPipeline:
    def __init__(self, df):
        self.df = df
    
    def preprocess(self):
        # Convert dates from string to date type
        self.df = self.df.withColumn("open_date", F.to_date(F.col("open_date"), "yyyy-MM-dd"))
        return self

    def add_account_longevity(self):
        self.df = self.df.withColumn("account_longevity", F.datediff(F.current_date(), F.col("open_date")))
        return self
    
    def add_balance_features(self):
        # Calculate mean, max, and min balances (curr_bal_amt, ledger_bal_amt)
        windowSpec = Window.partitionBy("hh_id_in_wh")
        self.df = self.df.withColumn("curr_bal_amt", F.col("curr_bal_amt").cast("float"))
        self.df = self.df.withColumn("ledger_bal_amt", F.col("ledger_bal_amt").cast("float"))

        balance_stats = self.df.groupBy("hh_id_in_wh").agg(
            F.mean("curr_bal_amt").alias("curr_bal_amt_mean"),
            F.max("curr_bal_amt").alias("curr_bal_amt_max"),
            F.min("curr_bal_amt").alias("curr_bal_amt_min"),
            F.mean("ledger_bal_amt").alias("ledger_bal_amt_mean"),
            F.stddev("ledger_bal_amt").alias("ledger_bal_amt_std"),
            F.max("ledger_bal_amt").alias("ledger_bal_amt_max"),
            F.min("ledger_bal_amt").alias("ledger_bal_amt_min")
        )
        self.df = self.df.join(balance_stats, "hh_id_in_wh", "left")
        return self
    
    def add_product_diversity(self):
        product_diversity = self.df.groupBy("hh_id_in_wh").agg(
            F.countDistinct("prd_code").alias("num_unique_products")
        )
        self.df = self.df.join(product_diversity, "hh_id_in_wh", "left")
        return self

    def add_temporal_features(self):
        self.df = self.df.withColumn("month", F.month("open_date"))
        self.df = self.df.withColumn("day_of_week", F.dayofweek("open_date"))
        self.df = self.df.withColumn("year", F.year("open_date"))
        return self

    def encode_categorical_data(self):
        indexer = StringIndexer(inputCol="prim_officer_code", outputCol="prim_officer_code_index")
        self.df = indexer.fit(self.df).transform(self.df)
        return self

    def assemble_features(self):
        feature_cols = [
            "account_longevity", "curr_bal_amt_mean", "product_diversity", "ledger_bal_amt_mean",
            "ledger_bal_amt_std", "ledger_bal_amt_max", "ledger_bal_amt_min", "num_unique_products",
            "month", "day_of_week", "year", "prim_officer_code_index"
        ]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        self.df = assembler.transform(self.df)
        return self.df

    def execute(self):
        self.preprocess()
        self.add_account_longevity()
        self.add_balance_features()
        self.add_product_diversity()
        self.add_temporal_features()
        self.encode_categorical_data()
        return self.assemble_features()

