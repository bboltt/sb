from pyspark.sql.functions import col, monotonically_increasing_id, sqrt
from pyspark.sql.window import Window

def calculate_similarity(spark, df, pwm_hh_ids, features):
    """
    Calculates cosine similarity between potential PWM clients and existing PWM clients, excluding self-comparisons.
    
    Args:
        spark (SparkSession): The Spark session object.
        df (DataFrame): DataFrame containing both PWM and non-PWM clients normalized features.
        pwm_hh_ids (DataFrame): DataFrame containing hh_id_in_wh of PWM clients.
        features (list of str): List of feature columns used for similarity calculation.

    Returns:
        DataFrame: A DataFrame with hh_id_in_wh and their top similarity scores.
    """
    # Normalize features in df
    df = normalize_features(df, features)

    # Assign unique IDs to facilitate exclusion of self-comparisons
    df = df.withColumn("unique_id", monotonically_increasing_id())

    # Join PWM client IDs back to the DataFrame, marking each row whether it's PWM or not
    pwm_df = df.join(pwm_hh_ids, "hh_id_in_wh").select("unique_id", "hh_id_in_wh", *features)
    non_pwm_df = df.join(pwm_hh_ids, "hh_id_in_wh", "left_anti").select("unique_id", "hh_id_in_wh", *features)

    # Perform cross join but exclude self-comparisons
    condition = (pwm_df["unique_id"] != non_pwm_df["unique_id"])
    cross_df = non_pwm_df.crossJoin(pwm_df).filter(condition)

    # Calculate dot products for cosine similarity
    dot_products = sum((non_pwm_df[f] * pwm_df[f]) for f in features)
    norms = sqrt(sum(non_pwm_df[f]**2 for f in features)) * sqrt(sum(pwm_df[f]**2 for f in features))

    cross_df = cross_df.withColumn("cosine_similarity", dot_products / norms)

    # Selecting top similarities using a window function
    windowSpec = Window.partitionBy(non_pwm_df["hh_id_in_wh"]).orderBy(col("cosine_similarity").desc())
    top_similarities = cross_df.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") <= 10)

    return top_similarities.select(non_pwm_df["hh_id_in_wh"], "cosine_similarity").distinct()



from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType
import numpy as np

def perform_clustering(df, features, k):
    """
    Performs K-Means clustering on the PWM data and returns the cluster centers.
    
    Args:
        df (DataFrame): DataFrame containing only PWM client data.
        features (list): List of feature names to include in the clustering.
        k (int): Number of clusters.
        
    Returns:
        DataFrame: DataFrame containing cluster centers.
    """
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    
    # Pipeline: Assemble features -> Normalize -> Cluster
    kmeans = KMeans(featuresCol="scaledFeatures", k=k)
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    
    model = pipeline.fit(df)
    centers = model.stages[-1].clusterCenters()
    
    # Convert cluster centers to DataFrame for easier processing in similarity calculation
    centers_df = spark.createDataFrame([Vectors.dense(center) for center in centers], ["features"])
    return centers_df

def calculate_cosine_similarity(v1, v2):
    """ Compute the cosine similarity between two vectors """
    return float(np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2)))

cosine_similarity_udf = udf(calculate_cosine_similarity, FloatType())

def calculate_similarity(df, cluster_centers, features):
    """
    Calculates the similarity of all clients to each cluster center.
    
    Args:
        df (DataFrame): DataFrame with all clients and their features.
        cluster_centers (DataFrame): DataFrame of cluster centers from K-Means.
        features (list): List of features to calculate similarity on.
    
    Returns:
        DataFrame: DataFrame with similarity scores.
    """
    # Ensure feature vectors are available in df
    df = VectorAssembler(inputCols=features, outputCol="features").transform(df)
    
    # Calculate similarity between each client and each cluster center
    for i, center in enumerate(cluster_centers.collect()):
        center_features = center["features"]
        df = df.withColumn(f"similarity_to_cluster_{i}", cosine_similarity_udf(col("features"), lit(center_features)))
    
    return df

