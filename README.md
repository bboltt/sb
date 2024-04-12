
# Private Wealth Management Project Demo

## Introduction

Welcome to our Private Wealth Management Project demonstration. Today, we'll discuss the project's architecture, the rationale behind our methodologies, and the structured approach we've adopted to enhance client identification and engagement.

## Project Strategy and Overview

The project is designed with modularity and flexibility in mind, allowing for easy adjustments and scalability. Our initial goal was to thoroughly understand our data and establish a versatile pipeline that accommodates various features and models.

## primary data source
```
dm_r3.pwm_mstr_dtl_daily
 |-- ar_id: long (nullable = true)
 |-- ip_id: long (nullable = true)
 |-- ar_name: string (nullable = true)
 |-- open_date: string (nullable = true)
 |-- close_date: string (nullable = true)
 |-- src_sys_code: string (nullable = true)
 |-- seg_code: string (nullable = true)
 |-- prim_officer_code: string (nullable = true)
 |-- prim_officer_asc_code: string (nullable = true)
 |-- prim_officer_asc_name: string (nullable = true)
 |-- ar_type_code: string (nullable = true)
 |-- st_code: string (nullable = true)
 |-- st_name: string (nullable = true)
 |-- cntry_code: string (nullable = true)
 |-- cntry_name: string (nullable = true)
 |-- city_name: string (nullable = true)
 |-- addr: string (nullable = true)
 |-- zip_code: string (nullable = true)
 |-- prd_code: string (nullable = true)
 |-- prd_name: string (nullable = true)
 |-- prd_dm_name: string (nullable = true)
 |-- prd_cls_name: string (nullable = true)
 |-- curr_bal_amt: decimal(17,2) (nullable = true)
 |-- ledger_bal_amt: decimal(17,2) (nullable = true)
 |-- ar_to_ip_relat_type_code: long (nullable = true)
 |-- ar_to_ip_relat_type_name: string (nullable = true)
 |-- ip_id_in_wh: string (nullable = true)
 |-- ip_name: string (nullable = true)
 |-- ip_tin: string (nullable = true)
 |-- rcif_cust_nbr: string (nullable = true)
 |-- hh_id: long (nullable = true)
 |-- hh_id_in_wh: string (nullable = true)
 |-- hh_name: string (nullable = true)
 |-- business_date: string (nullable = true)
```


## File Structure and Code Organization

- `data_loader.py`:
  - Responsible for loading and preprocessing data from various sources. 
- `feature_engineering_pipeline.py`:
  - Contains the `FeatureEngineeringPipeline` class which handles all transformations and feature engineering tasks. This class is crucial for preparing the data by creating meaningful features that are used in the model.

- `model_pipeline.py`:
  - This file includes functions for clustering PWM clients, calculating similarity scores, and ranking potential prospects. It encapsulates the core logic for identifying prospective PWM clients based on their similarity to existing clients.

- `main.py`:
  - The main script which executes workflows.
 


