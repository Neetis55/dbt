--dbt run --select ETL_MART_SALES_CORPORATION
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['CORPORATION_CODE']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_CORPORATION'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'Building ETL table CORPORATION for sales mart'
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_SALES'
        ,alias='CORPORATION'
        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]
        )
}}
with 
STG_XXON_CORP as (
SELECT 
    CORP_CODE AS CORPORATION_CODE
    ,CORP_DESC AS  CORPORATION_DESCRIPTION
    ,cust_account_id  AS ERP_CUST_ACCOUNT_ID
    ,NVL(customer_category_code,'CAT4') AS CORPORATION_CATEGORY_CODE
    ,CASE WHEN customer_category_code IS NULL THEN 'Foundation' ELSE category_description END  AS CORPORATION_CATEGORY_DESCRIPTION
    ,status AS ERP_STATUS_CODE
    ,customer_type AS CLASS 
    ,URL
    ,'N'::boolean IS_MARKET_MAKER -- first preference to customer table
    ,ACCOUNT_TYPE
    ,CUSTOMER_ITEM_CONTROL_REQUIRED::boolean as IS_CUSTOMER_ITEM_CONTROL_REQUIRED
    ,BARCODE_LABEL_FORMAT as BARCODE_LABEL_CODE
    ,VENDOR_CODE as VENDOR_CODE
    ,COLLECTOR as COLLECTOR_NAME
    ,TAX_PRINTING as TAX_PRINTING_DESCRIPTION
    ,GROUPING_RULE as GROUPING_RULE_CODE
    ,CREDIT_CHECK::boolean as HAS_CREDIT_CHECK
    ,CREDIT_HOLD::boolean as HAS_CREDIT_HOLD
    ,PRICE_LIST as CONTRACT_NAME
    ,BIW_INS_DTTM 
	,BIW_UPD_DTTM 
	,BIW_BATCH_ID
    FROM 
    {{ source('STG_EBS_APPS', 'XXON_OM_CORP_DETAILS_V') }}   
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY corp_code ORDER BY BIW_UPD_DTTM DESC)=1)
) 
,STG_CUSTOMER AS (
SELECT
    OID AS OID,
    account_num AS account_num, -- WE HAVE DUPLICATE ON ACCOUNT NAME
    custom_field28 AS custom_field28,
    CUSTOM_FIELD40,
    CUSTOM_FIELD41,
    DECODE (custom_field28,
                          'true', 'Y',
                          'false', 'N',
                          NULL
                         )::boolean AS IS_MARKET_MAKER
FROM 
{{ source('STG_MODELN_PRICING_OWNER', 'CUSTOMER') }}
WHERE customer_level_oid = 1
QUALIFY( ROW_NUMBER() OVER (PARTITION BY account_num ORDER BY MODIFIED_DATE desc, OID DESC)=1)
)
,STG_ORG_CUST_MAP AS(
select
    customer_oid ,
    org_oid
from 
{{ source('STG_MODELN_PRICING_OWNER', 'ORG_CUST_MAPPING') }}   
    -- Is_primary filter picks the recent data.
where is_primary =1 
QUALIFY( ROW_NUMBER() OVER (PARTITION BY customer_oid ORDER BY org_oid DESC)=1)
)
,STG_ORG as (
    SELECT
    OID AS OID,
    CODE AS ORG_CODE   
    FROM 
    {{ source('STG_MODELN_PRICING_OWNER', 'ORG') }}   
    WHERE
    lower(STATUS) = 'active' AND 
    IS_ORG_CUST_MAP =1 AND 
    OBSOLETE_FLAG=0
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY BIW_UPD_DTTM DESC)=1)
) 

,STG_SALES_TERRITORY AS(
    select
    ORG_CODE ,
	REGION_CODE  AS HOME_REGION
    from 
   {{ source('STG_MODELN_PRICING_ADMIN', 'SALES_TERRITORY') }}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY ORG_CODE ORDER BY BIW_UPD_DTTM DESC)=1)
)
,STG_PRO_USER AS(
    select
    OID,
    USERID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE,
    MODIFIED_BY,
    CREATED_BY
    from 
   {{ source('STG_MODELN_PRICING_ADMIN', 'PRO_USER') }}
   --Added filter to fetch only active user id and details
    where lower(STATUS) = 'active'
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY oid ORDER BY BIW_UPD_DTTM DESC)=1)
)
,STG_GAM AS (
    SELECT 
    GAM.account_num AS CORPORATION_CODE,
    PRO_USER.USERID AS GAM_USER_ID,
    PRO_USER.FIRST_NAME AS GAM_FIRST_NAME,
    PRO_USER.LAST_NAME AS GAM_LAST_NAME,
    PRO_USER.EMAIL AS GAM_EMAIL,
    PRO_USER.PHONE AS GAM_PHONE,
    PRO_USER.MODIFIED_BY AS GAM_CONTACT_LAST_UPDATE_BY,
    PRO_USER.CREATED_BY AS GAM_CONTACT_CREATED_BY
    FROM 
    STG_CUSTOMER  GAM
    INNER JOIN STG_PRO_USER PRO_USER
    ON GAM.CUSTOM_FIELD40 = PRO_USER.OID
    WHERE GAM.CUSTOM_FIELD40 IS NOT NULL
)
,STG_GSM AS (
    SELECT 
    GSM.account_num AS CORPORATION_CODE,
    PRO_USER.USERID AS GSM_USER_ID,
    PRO_USER.FIRST_NAME AS GSM_FIRST_NAME,
    PRO_USER.LAST_NAME AS GSM_LAST_NAME,
    PRO_USER.EMAIL AS GSM_EMAIL,
    PRO_USER.PHONE AS GSM_PHONE,
    PRO_USER.MODIFIED_BY AS GSM_CONTACT_LAST_UPDATE_BY,
    PRO_USER.CREATED_BY AS GSM_CONTACT_CREATED_BY
    FROM 
    STG_CUSTOMER  GSM
    INNER JOIN STG_PRO_USER PRO_USER
    ON GSM.CUSTOM_FIELD41 = PRO_USER.OID
    WHERE GSM.CUSTOM_FIELD41 IS NOT NULL
)
,FINAL_SQL AS(
  SELECT 
     STG.CORPORATION_CODE
    ,STG.CORPORATION_DESCRIPTION
    ,STG.ERP_CUST_ACCOUNT_ID
    ,STG.CORPORATION_CATEGORY_CODE
    ,STG.CORPORATION_CATEGORY_DESCRIPTION
    ,STG.ERP_STATUS_CODE
    ,SALES.HOME_REGION 
    ,coalesce(CUST.IS_MARKET_MAKER,stg.IS_MARKET_MAKER) as IS_MARKET_MAKER
    ,STG.CLASS 
    ,STG.URL
    ,STG.ACCOUNT_TYPE
    ,STG.IS_CUSTOMER_ITEM_CONTROL_REQUIRED 
    ,STG.BARCODE_LABEL_CODE
    ,STG.VENDOR_CODE
    ,STG.COLLECTOR_NAME
    ,STG.TAX_PRINTING_DESCRIPTION
    ,STG.GROUPING_RULE_CODE
    ,STG.HAS_CREDIT_CHECK
    ,STG.HAS_CREDIT_HOLD
    ,STG.CONTRACT_NAME
    ,GAM_USER_ID
    ,GAM_FIRST_NAME
    ,GAM_LAST_NAME
    ,GAM_EMAIL
    ,GAM_PHONE
    ,GAM_CONTACT_LAST_UPDATE_BY
    ,GAM_CONTACT_CREATED_BY
    ,GSM_USER_ID
    ,GSM_FIRST_NAME
    ,GSM_LAST_NAME
    ,GSM_EMAIL
    ,GSM_PHONE
    ,GSM_CONTACT_LAST_UPDATE_BY
    ,GSM_CONTACT_CREATED_BY,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID
	FROM STG_XXON_CORP STG
    LEFT JOIN STG_CUSTOMER CUST
    ON STG. CORPORATION_CODE = CUST.account_num
    -- BRIDGE BETWEEN CUSTOMER AND ORG
    LEFT JOIN STG_ORG_CUST_MAP ORG_CUST_MAP
    ON CUST.OID = ORG_CUST_MAP.CUSTOMER_OID
    LEFT JOIN STG_ORG ORG
    ON ORG_CUST_MAP.ORG_OID = ORG.OID
    -- GET REGION CODE
    LEFT JOIN STG_SALES_TERRITORY SALES
    ON ORG.ORG_CODE = SALES.ORG_CODE
    -- CONTACT DETAILS
    LEFT JOIN STG_GAM  GAM
    ON STG.CORPORATION_CODE = GAM.CORPORATION_CODE
    LEFT JOIN STG_GSM  GSM
    ON STG.CORPORATION_CODE = GSM.CORPORATION_CODE
)
SELECT 
*
,md5(object_construct ('col1',CORPORATION_DESCRIPTION, 'col2',ERP_CUST_ACCOUNT_ID, 'col3',CORPORATION_CATEGORY_CODE, 'col4',CORPORATION_CATEGORY_DESCRIPTION
 ,'col5',ERP_STATUS_CODE, 'col6',HOME_REGION, 'col7',IS_MARKET_MAKER, 'col8',CLASS, 'col9',URL, 'col10',ACCOUNT_TYPE, 'col11',IS_CUSTOMER_ITEM_CONTROL_REQUIRED
 , 'col12',BARCODE_LABEL_CODE, 'col13',VENDOR_CODE, 'col14',COLLECTOR_NAME, 'col15',TAX_PRINTING_DESCRIPTION, 'col16',GROUPING_RULE_CODE
 , 'col17',HAS_CREDIT_CHECK, 'col18',HAS_CREDIT_HOLD, 'col19',CONTRACT_NAME, 'col20',GAM_USER_ID, 'col21',GAM_FIRST_NAME, 'col22',GAM_LAST_NAME
 , 'col23',GAM_EMAIL, 'col24',GAM_PHONE, 'col25',GAM_CONTACT_LAST_UPDATE_BY, 'col26',GAM_CONTACT_CREATED_BY, 'col27',GSM_USER_ID, 'col28',GSM_FIRST_NAME
 , 'col29',GSM_LAST_NAME, 'col30',GSM_EMAIL, 'col31',GSM_PHONE, 'col32',GSM_CONTACT_LAST_UPDATE_BY, 'col33',GSM_CONTACT_CREATED_BY)::string ) as BIW_MD5_KEY
FROM FINAL_SQL
