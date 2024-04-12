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
from pyspark.sql.functions import monotonically_increasing_id

def perform_clustering(df, num_clusters):
    """
    Perform clustering on the scaled features and return cluster centers.
    
    Args:
        df (DataFrame): DataFrame with 'scaledFeatures' for clustering.
        num_clusters (int): Number of clusters to form.
    
    Returns:
        list: List of cluster centers as dense vectors.
    """
    kmeans = KMeans(featuresCol="scaledFeatures", k=num_clusters, seed=1)
    model = kmeans.fit(df)
    return model.clusterCenters()

def calculate_similarity_with_clusters(df_non_pwm, centers):
    """
    Calculate similarity of non-PWM clients to each cluster center.
    
    Args:
        df_non_pwm (DataFrame): DataFrame containing non-PWM clients with scaled features.
        centers (list): List of cluster centers.
    
    Returns:
        DataFrame: Updated DataFrame with similarity scores to each cluster.
    """
    # Assuming similarity calculation function is available
    for i, center in enumerate(centers):
        df_non_pwm = df_non_pwm.withColumn(f"similarity_to_cluster_{i}", cosine_similarity_udf("scaledFeatures", F.lit(center.tolist())))
    
    return df_non_pwm
