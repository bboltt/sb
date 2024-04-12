import pandas as pd
from optimal_spark_config.create_spark_instance import generate_spark_instance
from private_wealth_retention.segmentation_prospecting.data_loader import load_relevant_pwm_data, load_valid_switches, load_pwm_clients
from private_wealth_retention.segmentation_prospecting.feature_engineering_pipeline import FeatureEngineeringPipeline, normalize_features
from private_wealth_retention.segmentation_prospecting.model_pipeline import calculate_similarity, evaluate_prospects

def main():
    spark = generate_spark_instance(total_memory=600, total_vcpu=300)

    # Load data
    pwm_master_details_daily_df = load_relevant_pwm_data(spark)
    
    # Feature Engineering
    pipeline = FeatureEngineeringPipeline(pwm_master_details_daily_df)
    df_enriched = pipeline.execute()

    # Normalize features (assuming the features have already been added and preprocessed in df_enriched)
    feature_cols = ['account_longevity', 'curr_bal_amt_mean', 'product_diversity']  # Example feature columns
    df_enriched = normalize_features(df_enriched, feature_cols)

    # Split data into PWM and non-PWM clients
    pwm_clients_df = load_pwm_clients(spark)
    non_pwm_clients_df = df_enriched.join(pwm_clients_df, df_enriched["hh_id_in_wh"] == pwm_clients_df["hh_id_in_wh"], "left_anti")

    # Calculate Similarity
    similarity_df = calculate_similarity(spark, non_pwm_clients_df, pwm_clients_df, feature_cols)

    # Evaluate
    validswitches_df = load_valid_switches(spark)
    evaluation_df = evaluate_prospects(similarity_df, validswitches_df)

if __name__ == "__main__":
    main()
