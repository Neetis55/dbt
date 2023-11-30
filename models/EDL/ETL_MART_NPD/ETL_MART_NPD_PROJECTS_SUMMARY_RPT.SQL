/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_MART_NPD_PROJECTS_SUMMARY_RPT
-- dbt build --full-refresh --select ETL_MART_NPD_PROJECTS_SUMMARY_RPT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         17-NOV-2022     KALI DANDAPANI      Initial Version
1.1         03-JAN-2023     CLINT WHETTEN       Added DELEGATED_OWNER, BUSINESS_UNIT, PROJECT_PAL2
1.2         18-JAN-2023     CLINT WHETTEN       Added ITEM_NUMBER
1.3         19-JAN-2023     KALI DANDAPANI      Adding logic to override the source division and bu using MART_NPD.PROJECT_DIVISION_REF
1.4         20-JAN-2023     CLINT WHETTEN       Added NPD_COST_OBJECT
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PROJECT_SUMMARY_KEY']-%}

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
{%- set v_dbt_job_name = 'DBT_ETL_MART_NPD_PROJECTS_SUMMARY_RPT'-%}
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
         description = 'Building ETL table PROJECTS_SUMMARY_RPT for NPD LANDING PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_NPD'
        ,alias='PROJECTS_SUMMARY_RPT'
        ,tags =['MART_NPD']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH FISCAL_WEEK AS 
(
    SELECT 
        DISTINCT FISCAL_WEEK_KEY
    FROM 
    {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.ENTERPRISE.DATE  
    WHERE 
        CALENDAR_DATE = (CURRENT_TIMESTAMP() - INTERVAL '7 HOUR')::DATE
        or CALENDAR_DATE = (CURRENT_TIMESTAMP() )::DATE
)

,DIM_PPM_PROJECT_DTL AS (
    SELECT 
        PROJECT_NUMBER,
        PROJECT_ID,
        PROJECT_NAME,
        PROJECT_PRIORITY,
        IFF(PROJECT_PRIORITY IN ('1 Group Top Project','2 Division Top Project' ),1,0) AS TOP_PROJECT,
        DESCRIPTION PROJECT_DESCRIPTION,
        PROJECT_START_DATE,
        PROJECT_STATE,
        PROJECT_STATUS,
        ONSEMICONDUCTOR_STATUS,
        CURRENT_PHASE,
        DEVELOPMENT_CENTRE,
        PROGRAM_MANAGER,
        ENGR_PRODUCT_OWNER,
        PRODUCT_MRKT_LEAD,
        OWNER,
        DELEGATED_OWNER,
        PROJECT_SPONSOR  
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_PROJECT_DTL')}}  
    WHERE 
        PROJECT_TYPE = 'NPD'
        AND PROJECT_WIP_EXCEPTION IN ( 'PROJECT','EXCEPTION')
        AND ONSEMICONDUCTOR_STATUS NOT IN ('6 Cancelled (Customer)','7 Cancelled (Internal)' )
        AND PROJECT_STATE NOT IN ('Template','Proposed')
        AND PROJECT_STATUS IN ('In Process')
        AND CURRENT_PHASE NOT IN('6 Sustain','7 Retire')
        AND (PROJECT_NUMBER IS NOT NULL  OR LEN(PROJECT_NUMBER) <> 0)
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_NUMBER ORDER BY PROJECT_KEY DESC, BIW_UPD_DTTM DESC) =1)
)

,FACT_PPM_PTC_DTL AS (
    SELECT 
        PTC.PROJECT_NUMBER,
        PTC.PROJECT_ID,
        PTC.ITEM_NO AS ITEM_NUMBER
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','FACT_PPM_PTC_DTL')}} PTC
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PTC.PROJECT_NUMBER ORDER BY BIW_UPD_DTTM DESC) =1)
)

,BRIDGE_PPM_PROJECT_GROUP AS (
    SELECT 
        PROJECT_ID,
        GROUP_ID,
        DW_INSERT_DATE
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','BRIDGE_PPM_PROJECT_GROUP')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_ID ORDER BY DW_INSERT_DATE DESC, BIW_UPD_DTTM DESC) =1)
)

,DIM_PPM_GROUP AS (
    SELECT 
        GROUP_ID,
        GROUP_NAME  
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_GROUP')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY GROUP_ID ORDER BY DW_INSERT_DATE DESC, BIW_UPD_DTTM DESC) =1)
)

,BRIDGE_PPM_PROJECT_DIVISION AS (
    SELECT 
        PROJECT_ID,
        DIVISION_ID,
        DW_INSERT_DATE 
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','BRIDGE_PPM_PROJECT_DIVISION')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_ID ORDER BY DW_INSERT_DATE DESC, BIW_UPD_DTTM DESC) =1)
)

,DIM_PPM_DIVISION AS (
    SELECT 
        DIVISION_ID,
        DIVISION_NAME  
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_DIVISION')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY DIVISION_ID ORDER BY DW_INSERT_DATE DESC, BIW_UPD_DTTM DESC) =1)
)

,BRIDGE_PPM_PROJECT_BU AS (
    SELECT 
        PROJECT_ID,
        BUSINESS_UNIT_ID,
        DW_INSERT_DATE 
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','BRIDGE_PPM_PROJECT_BU')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PROJECT_ID ORDER BY DW_INSERT_DATE DESC, BIW_UPD_DTTM DESC) =1)
)

,DIM_PPM_BUSINESS_UNIT AS (
    SELECT 
        BUSINESS_UNIT_ID,
        BUSINESS_UNIT_NAME BUSINESS_UNIT,
        IFF(REGEXP_LIKE(BUSINESS_UNIT_NAME,'[a-zA-Z1-9]{2}\\s-.*'),LEFT(BUSINESS_UNIT_NAME,2),LEFT(RIGHT(BUSINESS_UNIT_NAME,3),2)) PROJECT_PAL2 
    FROM 
    {{source ('STG_PPM_ON02_NPD_MARTS','DIM_PPM_BUSINESS_UNIT')}}  
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY BUSINESS_UNIT_ID ORDER BY DW_INSERT_DATE DESC, BIW_UPD_DTTM DESC) =1)
)
--- reference table to override the source division and bu  
,PROJECT_DIVISION_REF AS 
(
    SELECT 
        OLD_BUSINESS_UNIT,
        OLD_DIVISION_DESCRIPTION,
        NEW_BUSINESS_UNIT,
        NEW_DIVISION_DESCRIPTION,
        NEW_DIVISION_CODE
    FROM 
    {{ref('MART_NPD_PROJECT_DIVISION_REF')}}
)

-- get the cost object name and project number from the npd cost model
,COST_OBJECT_NAME as (
    SELECT
        COSTOBJECTNAME AS NPD_COST_OBJECT,
        lfagileprojectnumber AS PROJECT_NUMBER
    FROM {{source ('STG_NPD_DBO','OB_PROJLF')}}
    WHERE length(lfagileprojectnumber) > 0
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY NPD_COST_OBJECT, PROJECT_NUMBER ORDER BY BIW_UPD_DTTM DESC) =1)
)
SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                            ,'COL2',PRJ.PROJECT_NUMBER::STRING
                         )::STRING 
        )::BINARY AS PROJECT_SUMMARY_KEY,
    FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY,
    PRJ.PROJECT_NUMBER,
    PRJ.PROJECT_ID,
    PRJ.PROJECT_NAME,
    PRJ.PROJECT_PRIORITY,
    PRJ.TOP_PROJECT,
    PRJ.PROJECT_DESCRIPTION,
    PRJ.PROJECT_START_DATE,
    PRJ.PROJECT_STATE,
    PRJ.PROJECT_STATUS,
    PRJ.ONSEMICONDUCTOR_STATUS,
    PRJ.CURRENT_PHASE,
    PRJ.DEVELOPMENT_CENTRE,
    PRJ.PROGRAM_MANAGER,
    PRJ.ENGR_PRODUCT_OWNER,
    PRJ.PRODUCT_MRKT_LEAD,
    PRJ.OWNER,
    PRJ.DELEGATED_OWNER,
    PRJ.PROJECT_SPONSOR,
    GRP.GROUP_NAME,
    REF_DIV.NEW_DIVISION_CODE AS DIVISION_ABBR,
    COALESCE(REF_DIV.NEW_DIVISION_DESCRIPTION, DIV.DIVISION_NAME) AS DIVISION_NAME,
    DTL.ITEM_NUMBER,
    COALESCE(REF_DIV.NEW_BUSINESS_UNIT,BU.BUSINESS_UNIT) AS BUSINESS_UNIT ,
    BU.PROJECT_PAL2,
    CON.NPD_COST_OBJECT,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    md5(object_construct ('col1',SNAPSHOT_WEEK_KEY::string, 'col2',PRJ.PROJECT_NUMBER::string, 'col3',PRJ.PROJECT_ID::string, 'col4',PRJ.PROJECT_NAME::string, 
    'col5',PROJECT_PRIORITY::string, 'col6',TOP_PROJECT::string, 'col7',PRJ.PROJECT_START_DATE::string, 'col8',PRJ.PROJECT_STATE::string, 'col9',PRJ.PROJECT_STATUS::string,
    'col10',PRJ.ONSEMICONDUCTOR_STATUS::string, 'col11',PRJ.CURRENT_PHASE::string, 'col12',PRJ.PROJECT_DESCRIPTION::string, 'col13',PRJ.DEVELOPMENT_CENTRE::string,
    'col14',PRJ.PROGRAM_MANAGER::string, 'col15',PRJ.ENGR_PRODUCT_OWNER::string, 'col16',PRJ.PRODUCT_MRKT_LEAD::string, 'col17',PRJ.OWNER::string,
    'col18',PRJ.DELEGATED_OWNER::string, 'col19',PRJ.PROJECT_SPONSOR::string, 'col20',GRP.GROUP_NAME::string, 
    'col21',COALESCE(REF_DIV.NEW_DIVISION_DESCRIPTION, DIV.DIVISION_NAME)::string, 'col22',REF_DIV.NEW_DIVISION_CODE::string, 'col23',DTL.ITEM_NUMBER::string, 
    'col24',COALESCE(REF_DIV.NEW_BUSINESS_UNIT,BU.BUSINESS_UNIT)::string, 'col25',BU.PROJECT_PAL2::string, 'col26', CON.NPD_COST_OBJECT::string)::string )::BINARY as BIW_MD5_KEY 
FROM     
DIM_PPM_PROJECT_DTL PRJ 
LEFT JOIN FACT_PPM_PTC_DTL DTL
    ON PRJ.PROJECT_ID = DTL.PROJECT_ID
LEFT JOIN BRIDGE_PPM_PROJECT_GROUP B_GRP
    ON PRJ.PROJECT_ID = B_GRP.PROJECT_ID
LEFT JOIN DIM_PPM_GROUP GRP
    ON B_GRP.GROUP_ID = GRP.GROUP_ID
LEFT JOIN BRIDGE_PPM_PROJECT_DIVISION B_DIV
    ON PRJ.PROJECT_ID = B_DIV.PROJECT_ID
LEFT JOIN DIM_PPM_DIVISION DIV
    ON B_DIV.DIVISION_ID = DIV.DIVISION_ID
--- Override the source division and bu using reference table
LEFT JOIN PROJECT_DIVISION_REF REF_DIV
    ON DIV.DIVISION_NAME = REF_DIV.OLD_DIVISION_DESCRIPTION
-- Add NPD Cost Model
LEFT JOIN COST_OBJECT_NAME CON
    ON PRJ.PROJECT_NUMBER = CON.PROJECT_NUMBER
LEFT JOIN BRIDGE_PPM_PROJECT_BU B_BU
    ON PRJ.PROJECT_ID = B_BU.PROJECT_ID
LEFT JOIN DIM_PPM_BUSINESS_UNIT BU
    ON B_BU.BUSINESS_UNIT_ID = BU.BUSINESS_UNIT_ID
CROSS JOIN FISCAL_WEEK FSC_WK