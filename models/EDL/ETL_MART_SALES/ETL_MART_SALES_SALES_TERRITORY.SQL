--dbt run --full-refresh --select ETL_MART_SALES_SALES_TERRITORY
--dbt run --select ETL_MART_SALES_SALES_TERRITORY
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['SALES_ORGANIZATION_CODE']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{% if is_incremental() %}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_SALES_TERRITORY'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'Building etl table SALES_TERRITORY for sales mart '
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_SALES'
        ,alias='SALES_TERRITORY'
        ,unique_key= v_pk_list
        ,merge_update_columns = ['SALES_ORGANIZATION_DESCRIPTION', 'SALES_REP_FIRM_NAME', 'SALES_OFFICE_CODE', 'SALES_OFFICE_NAME'
                                , 'SALES_AREA_NAME', 'SALES_SUB_AREA_NAME', 'SALES_REGION_CODE', 'SALES_REGION_NAME', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
        ,tags ='MART_SALES'
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

with 
STG_ORG as (
    SELECT 
    OBSOLETE_FLAG AS OBSOLETE_FLAG ,
    NAME AS SALES_ORGANIZATION_DESCRIPTION ,
    IS_ORG_CUST_MAP AS IS_ORG_CUST_MAP ,
    CODE AS SALES_ORGANIZATION_CODE ,
    STATUS AS STATUS   
    FROM 
    {{ source('STG_MODELN_PRICING_OWNER', 'ORG') }}   
    WHERE
    lower(STATUS) = 'active' AND 
    IS_ORG_CUST_MAP =1 AND 
    OBSOLETE_FLAG=0
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY BIW_UPD_DTTM DESC)=1)
) 
,
STG_OFFICE AS (
    SELECT
    NAME AS SALES_OFFICE_NAME,
    CODE
    FROM 
    {{ source('STG_MODELN_PRICING_ADMIN', 'SALES_OFFICE') }}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY BIW_UPD_DTTM DESC)=1)
)
,
STG_REGION AS(
    select
    NAME AS SALES_REGION_NAME,
    CODE
    from 
    {{ source('STG_MODELN_PRICING_ADMIN', 'SALES_REGION') }}    
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY BIW_UPD_DTTM DESC)=1)
)
,
STG_TERRITORY AS
(
    select
    ORG_CODE AS SALES_ORGANIZATION_CODE,
	REP_FIRM_NAME AS SALES_REP_FIRM_NAME,
	OFFICE_CODE AS SALES_OFFICE_CODE,
	AREA_NAME AS SALES_AREA_NAME,
	SUB_AREA_NAME AS SALES_SUB_AREA_NAME,
	REGION_CODE AS SALES_REGION_CODE,
    BIW_INS_DTTM ,
	BIW_UPD_DTTM ,
	BIW_BATCH_ID
    from 
   {{ source('STG_MODELN_PRICING_ADMIN', 'SALES_TERRITORY') }}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY ORG_CODE ORDER BY BIW_UPD_DTTM DESC)=1)
),
FINAL_SQL AS
(
 select 	 
	STG.SALES_ORGANIZATION_CODE,
    ORG.SALES_ORGANIZATION_DESCRIPTION,
	STG.SALES_REP_FIRM_NAME,
	STG.SALES_OFFICE_CODE,
	OFFICE.SALES_OFFICE_NAME,
	STG.SALES_AREA_NAME,
	STG.SALES_SUB_AREA_NAME,
	STG.SALES_REGION_CODE,
	REGION.SALES_REGION_NAME,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID
    from	
    STG_TERRITORY   STG
    LEFT OUTER JOIN  STG_OFFICE OFFICE
    ON  STG.SALES_OFFICE_CODE = OFFICE.CODE
    LEFT OUTER JOIN  STG_REGION REGION
    ON  STG.SALES_REGION_CODE = REGION.CODE
    LEFT JOIN STG_ORG ORG
    ON STG.SALES_ORGANIZATION_CODE = ORG.SALES_ORGANIZATION_CODE
)
SELECT *
,md5(object_construct ('col1',SALES_ORGANIZATION_DESCRIPTION, 'col2',SALES_REP_FIRM_NAME, 'col3',SALES_OFFICE_CODE, 'col4',SALES_OFFICE_NAME
, 'col5',SALES_AREA_NAME, 'col6',SALES_SUB_AREA_NAME, 'col7',SALES_REGION_CODE, 'col8',SALES_REGION_NAME)::string 
) as BIW_MD5_KEY
FROM FINAL_SQL
