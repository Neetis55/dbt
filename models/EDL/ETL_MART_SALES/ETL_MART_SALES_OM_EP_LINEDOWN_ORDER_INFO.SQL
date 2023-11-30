/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select DBT_MART_OUTSYS_OM_EP_LINEDOWN_ORDER_INFO 

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         11/13/2022      Alfred        Initial Version 
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['LINEDOWN_ORDER_RULE_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}
 

{################# Batch control insert and update SQL #################}
-- Step 1 Batch process info  
{%- set v_dbt_job_name = 'ETL_MART_SALES_LINEDOWN_ORDER_INFO'-%}
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}


{################# Snowflake Object Configuration #################}
{{
    config(
         description = 'Building ETL MART OM_EP_LINEDOWN_INFO table' 
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_SALES' 
        ,alias = 'MART_OM_EP_LINEDOWN_ORDER_INFO'
        ,unique_key= v_pk_list
        ,merge_update_columns = ['PONumber', 'POINumber', 'BIW_INS_DTTM', 'BIW_UPD_DTTM', 'BIW_BATCH_ID' ] 
        ,tags ='MART_OUTSYS_SALES'
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH 

STG AS (
    select  --list of PK in md5 construct
            MD5(OBJECT_CONSTRUCT ('COL1',LindownId::STRING)::STRING ) AS LINEDOWN_ORDER_RULE_KEY,
            LindownId,
            PONumber,
            POINumber from {{ source('STG_OUTSYS','STG_OM_EP_LINEDOWN_ORDER_INFO') }} src   
    where  
        BIW_UPD_DTTM >= '{{V_LWM}}'
	 	AND BIW_UPD_DTTM < '{{V_HWM}}' 
),

final AS (
    select  LINEDOWN_ORDER_RULE_KEY,
            LindownId,
            PONumber,
            POINumber,
            '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
            '{{V_START_DTTM}}'::TIMESTAMP_NTZ as BIW_UPD_DTTM, 
            '{{V_BIW_BATCH_ID}}' as BIW_BATCH_ID,
            --list of all objects except BIW columns
            md5(object_construct (  'col1',LINEDOWN_ORDER_RULE_KEY::string, 'col2',LindownId::string,
                                    'col3',PONumber::string, 'col4',POINumber::string)
                ::string) as BIW_MD5_KEY
            from STG
)

select * from final

