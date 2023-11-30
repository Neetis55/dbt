/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_SALES_POINT_OF_SALES_FACT_FCS_ONE_TIME_LOAD --vars 'is_one_time_load: True'

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         09/26/2022      Vinayaka        Initial Version
2.0         10/18/2022      Sruthi Kasbe    Updated customer columns and its order
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['TRANSACTION_KEY']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
/*--DBT Variable used during development*/
{% if is_incremental() %}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
{%-set v_update_column_list = edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
/*--DBT Variable*/
/*--SELECT {{v_all_column_list}}*/
/*--SELECT {{v_update_column_list}}*/
/*--SELECT {{v_md5_column_list}}*/
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_POINT_OF_SALES_FACT_FCS_ONE_TIME_LOAD'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}


{################# Snowflake Object Configuration #################}
{{
    config(
         description = 'Building one time ETL table FCS_POS_FACT for Sales Mart '
        ,transient=true
        ,materialized='view'
        ,schema ='ETL_MART_SALES'
        ,alias='POINT_OF_SALES_FACT_FCS'
        ,tags =['MART_SALES']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH DIM_CUSTOMERS AS (
  SELECT 
    KEY_CUSTOMER,
    CUST_CD
  FROM 
  {{ source('STG_SDM_CORE', 'DIM_CUSTOMERS') }} 
  QUALIFY(ROW_NUMBER() OVER (PARTITION BY CUST_CD ORDER BY BIW_UPD_DTTM DESC) =1)
),
DIM_FCS_CUSTOMERS AS (
  SELECT 
    KEY_CUSTOMER,
    CUST_CD
  FROM 
    {{ source('STG_SDM_PRESENTATION', 'DIM_FCSCUSTOMERS') }}
  QUALIFY(ROW_NUMBER() OVER (PARTITION BY CUST_CD ORDER BY BIW_UPD_DTTM DESC) =1)
),
DIM_CORP AS (
  select 
    KEY_CORPORATION,
    CORP_CD
  from 
    {{ source('STG_SDM_CORE', 'LKP_CORPORATIONS') }}
    QUALIFY(ROW_NUMBER() OVER (PARTITION BY CORP_CD ORDER BY BIW_UPD_DTTM DESC) =1)
),
VW_PRODUCTS AS (
    SELECT KEY_PRODUCT ,
        PRODUCT_ID ,
        PAL3_CD
    FROM   {{ source('STG_SDM_PRESENTATION', 'VW_PRODUCTS') }}
--QUALIFY(ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY BIW_UPD_DTTM DESC) =1)
),
FACT_FCSPOS AS (
    SELECT 
    KEY_POSDAILY::VARCHAR AS TRANS_ID,  
--DATE  
    TRANSACTION_DATE,
    CREATE_DATE,
--CODE  
    PRODUCT_ID,
    --CUSTOMER
    DIR_CUST_CD,
    IND_CUST_CD,
-- KEY
    KEY_PRODUCT,  
    -- CUSTOMER
    KEY_ENDCUSTOMER,
    --CORPORATION
    KEY_ENDCORPORATION,
    KEY_INDCORPORATION, 
--OTHERS
    STRESALE_QTY,
    CURRENCY_ISO_CD,
    STRESALE_NET_AMT,
    ST_DIST_PRICE_AMT,
-- DISTIRIBUTOR 
    DISTSHIPTOCUSTID,
    DISTSHIPTOCUSTNAME,
    DISTENDCUSTID  ,
    DISTENDCUSTNAME,
    DIST_INVOICE_NUM ,
    DIST_PO_NUM 
    FROM {{ source('STG_SDM_PRESENTATION', 'FACT_FCSPOS') }}
    QUALIFY(ROW_NUMBER() OVER (PARTITION BY KEY_POSDAILY ORDER BY BIW_UPD_DTTM DESC) =1)
),
FINAL_CTE AS (
  SELECT  
    --KEY
      MD5(COALESCE(FCS.TRANS_ID,'-1')::STRING) AS TRANSACTION_KEY ,     
      REPLACE(TRANSACTION_DATE,'-')::NUMBER AS FISCAL_SHIP_DATE_KEY ,
      
      --CUSTOMER
      MD5(COALESCE(FCS.DIR_CUST_CD,'-1')::STRING) AS DIRECT_CUSTOMER_KEY ,
      MD5(COALESCE(FCS.IND_CUST_CD,'-1')::STRING) AS INDIRECT_CUSTOMER_KEY ,
      MD5(COALESCE(ECUST.CUST_CD,EFCSCUST.CUST_CD,FCS.KEY_ENDCUSTOMER::STRING,'-1')::STRING) AS END_CUSTOMER_KEY , 
    --COALESCE(MD5(EFCSCUST.CUST_CD),FCS.KEY_ENDCUSTOMER::STRING) AS END_CUSTOMER_KEY , 
      --COROPORATION
      md5('-1') AS DIRECT_CORPORATION_KEY ,
      MD5(COALESCE(INDCORP.CORP_CD,FCS.KEY_INDCORPORATION::STRING,'-1')::STRING) AS INDIRECT_CORPORATION_KEY ,
      MD5(COALESCE(ECORP.CORP_CD,FCS.KEY_ENDCORPORATION::STRING,'-1')::STRING) AS END_CORPORATION_KEY ,
      MD5(COALESCE(FCS.PRODUCT_ID,'-1')::STRING) AS MARKET_PRODUCT_NUMBER_KEY ,
      md5('-1') as INTERNAL_PART_NUMBER_KEY,
      FCS.TRANS_ID AS TRANSACTION_ID ,
      FCS.TRANSACTION_DATE AS FISCAL_SHIP_DATE ,
    --CODE
      FCS.DIR_CUST_CD AS DIRECT_CUSTOMER_CODE ,
      FCS.IND_CUST_CD AS INDIRECT_CUSTOMER_CODE ,
      ECUST.CUST_CD AS END_CUSTOMER_CODE ,
      NULL AS DIRECT_CORPORATION_CODE ,
      INDCORP.CORP_CD AS INDIRECT_CORPORATION_CODE ,
      ECORP.CORP_CD AS END_CORPORATION_CODE ,
      FCS.PRODUCT_ID AS MARKET_PRODUCT_NUMBER ,
      null as INTERNAL_PART_NUMBER,
      NULL AS TRANSACTION_CODE ,
  --OTHERS
      NULL AS REPORTED_SHIP_DATE ,
      NULL AS REPORTED_BRANCH_ID ,
      FCS.STRESALE_QTY AS RESALE_QUANTITY ,
      --DISTRIBUTOR
      FCS.DISTSHIPTOCUSTID AS DISTRIBUTOR_SHIP_TO_CUSTOMER_IDENTIFIER ,
      FCS.DISTSHIPTOCUSTNAME AS DISTRIBUTOR_SHIP_TO_CUSTOMER_NAME ,
      FCS.DISTENDCUSTID AS DISTRIBUTOR_END_CUSTOMER_IDENTIFIER ,
      FCS.DISTENDCUSTNAME AS DISTRIBUTOR_END_CUSTOMER_NAME ,
      FCS.DIST_INVOICE_NUM AS DISTRIBUTOR_INVOICE_NUMBER ,
      NULL AS DISTRIBUTOR_INVOICE_LINE_NUMBER ,
      NULL AS REGISTRATION_NUMBER ,
      NULL AS DEBIT_NUMBER ,
      FCS.DIST_PO_NUM AS DISTRIBUTOR_PO_NUMBER ,
      FCS.CURRENCY_ISO_CD AS TRANSACTION_CURRENCY_ISO_CODE ,
    --RESALE
      NULL AS RESALE_UNIT_PRICE ,
      NULL AS RESALE_UNIT_PRICE_USD ,
      FCS.STRESALE_NET_AMT AS RESALE_NET_AMOUNT ,
      NULL AS RESALE_NET_AMOUNT_USD ,
      NULL AS RESALE_NET_AMOUNT_EUR ,
      NULL AS RESALE_NET_AMOUNT_JPY ,
      NULL AS DISTRIBUTOR_UNIT_PRICE ,
      NULL AS DISTRIBUTOR_UNIT_PRICE_USD ,
      FCS.ST_DIST_PRICE_AMT AS DISTRIBUTOR_COST_AMOUNT ,
      NULL AS DISTRIBUTOR_COST_AMOUNT_USD ,
      NULL AS DISTRIBUTOR_COST_AMOUNT_EUR ,
      NULL AS DISTRIBUTOR_COST_AMOUNT_JPY ,
      'FAIRCHILD' AS SOURCE_DATA_FROM ,
      FCS.CREATE_DATE AS ONSALES_CREATE_DATE ,
      FCS.CREATE_DATE AS ONSALES_MODIFIED_DATE ,
      TRUE::BOOLEAN AS IS_TRUE_POS ,
      NULL AS IS_SDM_TRUE_POS ,
      FALSE::BOOLEAN AS IS_VAR ,
      NULL AS END_CORPORATION_DECODE ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
     {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID     ,
      'N'::BOOLEAN BIW_LOGICAL_DELETE_FLAG
  FROM FACT_FCSPOS FCS 
  LEFT JOIN VW_PRODUCTS PROD ON PROD.KEY_PRODUCT = FCS.KEY_PRODUCT
  -- CUST CODE
  LEFT JOIN DIM_CUSTOMERS ECUST ON FCS.KEY_ENDCUSTOMER = ECUST.KEY_CUSTOMER
  LEFT JOIN DIM_FCS_CUSTOMERS EFCSCUST ON FCS.KEY_ENDCUSTOMER = EFCSCUST.KEY_CUSTOMER
  -- CORPORATION
  LEFT JOIN DIM_CORP INDCORP ON FCS.KEY_INDCORPORATION = INDCORP.KEY_CORPORATION
  LEFT JOIN DIM_CORP ECORP ON FCS.KEY_ENDCORPORATION = ECORP.KEY_CORPORATION
  WHERE PROD.PAL3_CD<>'F9F'
)
SELECT 
  FINAL_CTE.*
   , md5(object_construct ('col1',TRANSACTION_KEY::string, 'col2',FISCAL_SHIP_DATE_KEY::string,
    'col3',DIRECT_CUSTOMER_KEY::string, 'col4',INDIRECT_CUSTOMER_KEY::string, 'col5',END_CUSTOMER_KEY::string, 'col6',DIRECT_CORPORATION_KEY::string,
    'col7',INDIRECT_CORPORATION_KEY::string, 'col8',END_CORPORATION_KEY::string, 'col9',MARKET_PRODUCT_NUMBER_KEY::string, 'col10',INTERNAL_PART_NUMBER_KEY::string,
    'col11',FISCAL_SHIP_DATE::string, 'col12',MARKET_PRODUCT_NUMBER::string, 'col13',INTERNAL_PART_NUMBER::string, 'col14',DIRECT_CUSTOMER_CODE::string,
    'col15',INDIRECT_CUSTOMER_CODE::string, 'col16',END_CUSTOMER_CODE::string, 'col17',DIRECT_CORPORATION_CODE::string, 'col18',INDIRECT_CORPORATION_CODE::string,
    'col19',END_CORPORATION_CODE::string, 'col20',TRANSACTION_CODE::string, 'col21',REPORTED_SHIP_DATE::string, 'col22',REPORTED_BRANCH_ID::string,
    'col23',RESALE_QUANTITY::string, 'col24',DISTRIBUTOR_SHIP_TO_CUSTOMER_IDENTIFIER::string, 'col25',DISTRIBUTOR_SHIP_TO_CUSTOMER_NAME::string,
    'col26',DISTRIBUTOR_END_CUSTOMER_IDENTIFIER::string, 'col27',DISTRIBUTOR_END_CUSTOMER_NAME::string, 'col28',DISTRIBUTOR_INVOICE_NUMBER::string,
    'col29',DISTRIBUTOR_INVOICE_LINE_NUMBER::string, 'col30',REGISTRATION_NUMBER::string, 'col31',DEBIT_NUMBER::string, 'col32',DISTRIBUTOR_PO_NUMBER::string,
    'col33',TRANSACTION_CURRENCY_ISO_CODE::string, 'col34',RESALE_UNIT_PRICE::string, 'col35',RESALE_UNIT_PRICE_USD::string, 'col36',RESALE_NET_AMOUNT::string,
    'col37',RESALE_NET_AMOUNT_USD::string, 'col38',RESALE_NET_AMOUNT_EUR::string, 'col39',RESALE_NET_AMOUNT_JPY::string, 'col40',DISTRIBUTOR_UNIT_PRICE::string,
    'col41',DISTRIBUTOR_UNIT_PRICE_USD::string, 'col42',DISTRIBUTOR_COST_AMOUNT::string, 'col43',DISTRIBUTOR_COST_AMOUNT_USD::string,
    'col44',DISTRIBUTOR_COST_AMOUNT_EUR::string, 'col45',DISTRIBUTOR_COST_AMOUNT_JPY::string, 'col46',SOURCE_DATA_FROM::string, 'col47',ONSALES_CREATE_DATE::string,
    'col48',ONSALES_MODIFIED_DATE::string, 'col49',IS_TRUE_POS::string, 'col50',IS_SDM_TRUE_POS::string, 'col51',IS_VAR::string,
    'col52',END_CORPORATION_DECODE::string, 'col53',BIW_LOGICAL_DELETE_FLAG::string)::string ) as BIW_MD5_KEY
FROM 
  FINAL_CTE