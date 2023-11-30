/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_SALES_OM_DEMAND_SHAPING_RULES

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         09 NOV 2022     VINAY SUBRAMANIAN   Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['DEMAND_SHAPING_RULE_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_OM_DEMAND_SHAPING_RULES'-%}
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
         description = 'Building table OM_DEMAND_SHAPING_RULES for MART_SALES'
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_SALES'
        ,alias='OM_DEMAND_SHAPING_RULES'
		,unique_key= v_pk_list
        ,tags =['MART_SALES']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH DEMAND_SHAPING_RULES AS (
    SELECT
        MD5(OBJECT_CONSTRUCT ('COL1',RULE_NM::STRING
                            , 'COL2',LEVEL_NM::STRING
                            , 'COL3',LEVEL_VAL::STRING
                            , 'COL4',LEVEL_VAL2::STRING
                            , 'COL5',LEVEL_VAL3::STRING)::STRING ) AS DEMAND_SHAPING_RULE_KEY,
        RULE_NM AS RULE_NAME,
        LEVEL_NM AS LEVEL_NAME,
        LEVEL_VAL AS LEVEL_VALUE1,
        RULE_VAL AS RULE_VALUE,
        ACTIVE_START_DT AS RULE_ACTIVE_START_DATE,
        ACTIVE_END_DT_ACTIVE AS RULE_ACTIVE_END_DATE,
        CREATION_DTME AS RULE_CREATION_DATETIME,
        CREATED_BY AS RULE_CREATED_BY,
        LAST_UPDATE_DTME AS RULE_LAST_UPDATED_DATETIME,
        LAST_UPDATED_BY AS RULE_LAST_UPDATED_BY,
        LEVEL_VAL2 AS LEVEL_VALUE2,
        LEVEL_VAL3 AS LEVEL_VALUE3
        FROM {{source('STG_EBS_APPS','XXON_OM_DEMAND_SHAPPING_RULES')}} 
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY RULE_NM,LEVEL_NM,LEVEL_VAL,LEVEL_VAL2,LEVEL_VAL3 ORDER BY BIW_UPD_DTTM DESC )=1)                         
)

SELECT 
    DEMAND_SHAPING_RULE_KEY,
    RULE_NAME,
    LEVEL_NAME,
    LEVEL_VALUE1,
    RULE_VALUE,
    RULE_ACTIVE_START_DATE,
    RULE_ACTIVE_END_DATE,
    RULE_CREATION_DATETIME,
    RULE_CREATED_BY,
    RULE_LAST_UPDATED_DATETIME,
    RULE_LAST_UPDATED_BY,
    LEVEL_VALUE2,
    LEVEL_VALUE3,
    CASE WHEN RULE_ACTIVE_END_DATE >= TO_DATE('31/12/2050','DD/MM/YYYY') OR RULE_ACTIVE_END_DATE IS NULL
         THEN 'Y' 
         ELSE 'N'
    END::BOOLEAN AS IS_RULE_ACTIVE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    md5(object_construct ('col1',DEMAND_SHAPING_RULE_KEY::string, 'col2',RULE_NAME::string,
    'col3',LEVEL_NAME::string, 'col4',LEVEL_VALUE1::string, 'col5',RULE_VALUE::string, 'col6',RULE_ACTIVE_START_DATE::string,
    'col7',RULE_ACTIVE_END_DATE::string, 'col8',RULE_CREATION_DATETIME::string, 'col9',RULE_CREATED_BY::string,
    'col10',RULE_LAST_UPDATED_DATETIME::string, 'col11',RULE_LAST_UPDATED_BY::string, 'col12',LEVEL_VALUE2::string, 'col13',LEVEL_VALUE3::string,
    'col14',IS_RULE_ACTIVE::string)::string) as BIW_MD5_KEY
    FROM DEMAND_SHAPING_RULES