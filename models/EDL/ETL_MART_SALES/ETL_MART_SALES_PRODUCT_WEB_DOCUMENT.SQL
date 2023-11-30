/*---------------------------------------------------------------------------
Command to run model:
dbt run --select ETL_MART_SALES_PRODUCT_WEB_DOCUMENT
dbt run --full-refresh --select ETL_MART_SALES_PRODUCT_WEB_DOCUMENT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         06 DEC 2022      VINAYAKA cn      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PRODUCT_WEB_DOCUMENT_KEY']-%}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_PRODUCT_WEB_DOCUMENT'-%}
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
         description = 'Building ETL table PART WEB for SALES Mart'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_SALES'
        ,alias='PRODUCT_WEB_DOCUMENT'
        ,tags =['MART_SALES']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}


WITH EWPC_BASE_PART AS
(
    SELECT 
        BASE_PART,
        WEB_VISIBLE
    FROM 
        {{source ('STG_WEBPRD_PUBLIC','EWPC_BASE_PART')}}
    WHERE WEB_VISIBLE='T' 
    QUALIFY( row_number() OVER (PARTITION BY BASE_PART ORDER BY BIW_UPD_DTTM DESC)=1)
),

EWPC_ORDERABLE_PART AS
(
    SELECT 
        ORDERABLE_PART ,
        BASE_PART
    FROM {{source ('STG_WEBPRD_PUBLIC','EWPC_ORDERABLE_PART')}}
    QUALIFY( row_number() OVER (PARTITION BY ORDERABLE_PART ORDER BY BIW_UPD_DTTM DESC)=1)
),

EWPC_PART_DOCUMENT AS (
    SELECT 
        DOC_ID ,
        BASE_PART 
    FROM 
        {{source ('STG_WEBPRD_PUBLIC','EWPC_PART_DOCUMENT')}} 
    QUALIFY(ROW_NUMBER() OVER (PARTITION BY DOC_ID ,BASE_PART ORDER BY BIW_UPD_DTTM)=1)
) ,

EWPC_DOCUMENT AS(
    SELECT 
        DOC_ID ,
        DOC_TYPE,
        DOC_NAME ,
        DOC_FILE_NAME 
    FROM 
        {{source ('STG_WEBPRD_PUBLIC','EWPC_DOCUMENT')}}
    WHERE DOC_TYPE IN ('Drawing: Package Drawing')
    QUALIFY(ROW_NUMBER() OVER (PARTITION BY DOC_ID ORDER BY BIW_UPD_DTTM)=1)
) 

SELECT 
    MD5(OBJECT_CONSTRUCT( 'COL1' , ORD_PAR.ORDERABLE_PART::STRING
                        , 'COL2' , DOC.DOC_TYPE::STRING
                        , 'COL3' , DOC.DOC_NAME::STRING)::STRING
            )               AS PRODUCT_WEB_DOCUMENT_KEY,
    ORD_PAR.ORDERABLE_PART  AS PRODUCT_ID ,
    PAR_DOC.BASE_PART       AS BASE_PART ,
    DOC.DOC_NAME            AS DOCUMENT_NAME ,
    DOC.DOC_FILE_NAME       AS DOCUMENT_FILE_NAME ,
    DOC.DOC_TYPE            AS DOCUMENT_TYPE ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ AS BIW_INS_DTTM ,    
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ AS BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}      AS BIW_BATCH_ID ,
    MD5(OBJECT_CONSTRUCT ('col1',PRODUCT_ID::string, 'col2',PAR_DOC.BASE_PART::string, 'col3',DOCUMENT_NAME::string
        , 'col4',DOCUMENT_FILE_NAME::string, 'col5',DOCUMENT_TYPE::string)::string ) AS BIW_MD5_KEY
    
FROM EWPC_PART_DOCUMENT PAR_DOC
    INNER JOIN EWPC_DOCUMENT DOC   
        ON DOC.DOC_ID = PAR_DOC.DOC_ID
	INNER JOIN EWPC_BASE_PART BAS_PAR
        ON PAR_DOC.BASE_PART = BAS_PAR.BASE_PART 
    INNER JOIN EWPC_ORDERABLE_PART ORD_PAR 
        ON BAS_PAR.BASE_PART = ORD_PAR.BASE_PART
        