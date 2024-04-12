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
