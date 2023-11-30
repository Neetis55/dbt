/*---------------------------------------------------------------------------
Command to run model:
dbt run --select ETL_MART_SALES_PRODUCT
dbt build --select ETL_MART_SALES_PRODUCT

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.1         24-NoV-2022     Vinayaka CN    Added product attributes like IS_LAUNCH_SALES_TIPS ,IS_LAUNCH_PRESENTATION_CUSTOMER ,IS_LAUNCH_PRESENTATION_DISTRIBUTOR ,IS_DATA_SHEET_AVAILABLE
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PRODUCT_ID']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
---SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_PRODUCT'-%}
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
         description = 'Building ETL table PRODUCT for Mart Sales'
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_SALES'
        ,alias='PRODUCT'
        ,tags =['MART_SALES']
        ,post_hook= [v_sql_upd_success_batch]
        )
}}

With PRICING_OWNER_PART AS 
(
    SELECT
        MPN,
        MULT,
        OID,
        CREATED_DATE
    FROM {{source ('STG_MODELN_PRICING_OWNER','PART')}}
    QUALIFY( row_number() OVER (PARTITION BY MPN ORDER BY BIW_UPD_DTTM DESC)=1) 
),

-------------WEB ATTRIBUTE TABLES
EWPC_BASE_PART AS
(
    SELECT 
        BASE_PART,
        SHORT_DESC,
        WEB_VISIBLE
    FROM {{source ('STG_WEBPRD_PUBLIC','EWPC_BASE_PART')}}
    QUALIFY( row_number() OVER (PARTITION BY BASE_PART ORDER BY BIW_UPD_DTTM DESC)=1)
),

EWPC_ORDERABLE_PART AS
(
    SELECT 
        BASE_PART,
        ORDERABLE_PART,
        PAL_CODE,
        WEB_VISIBLE ,
        STATUS
    FROM {{source ('STG_WEBPRD_PUBLIC','EWPC_ORDERABLE_PART')}}
    QUALIFY( row_number() OVER (PARTITION BY ORDERABLE_PART ORDER BY BIW_UPD_DTTM DESC)=1)
),

EWPC_PART_DOCUMENT_SEC AS (
    SELECT 
        DOC_ID ,
        BASE_PART 
    FROM 
        {{source ('STG_WEBPRD_PUBLIC','EWPC_PART_DOCUMENT_SEC')}} 
    QUALIFY(ROW_NUMBER() OVER (PARTITION BY DOC_ID ,BASE_PART ORDER BY BIW_UPD_DTTM)=1)
) ,

EWPC_DOCUMENT_SEC AS(
    SELECT 
        DOC_ID ,
        WEB_VISIBLE ,
        DOC_TYPE
    FROM 
        {{source ('STG_WEBPRD_PUBLIC','EWPC_DOCUMENT_SEC')}}
    QUALIFY(ROW_NUMBER() OVER (PARTITION BY DOC_ID ORDER BY BIW_UPD_DTTM)=1)
) ,

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
        WEB_VISIBLE ,
        DOC_TYPE
    FROM 
        {{source ('STG_WEBPRD_PUBLIC','EWPC_DOCUMENT')}}
    QUALIFY(ROW_NUMBER() OVER (PARTITION BY DOC_ID ORDER BY BIW_UPD_DTTM)=1)
) ,

---------- GET PRODUCT DOCUMENT TYPE
PRODUCT_DOC_TYPE AS (
    SELECT 
        O_PART.ORDERABLE_PART ,
        MAX(CASE 
            WHEN   DOC_SEC.DOC_TYPE = 'Launch Sales Tips'
                THEN 1 
            ELSE 0
        END) AS LAUNCH_SALES_TIPS,
        MAX(CASE 
            WHEN  DOC_SEC.DOC_TYPE = 'Launch Product Presentation'
                THEN 1 
            ELSE 0
        END) AS LAUNCH_PRESENTATION_CUSTOMER,
        MAX(CASE 
            WHEN  DOC_SEC.DOC_TYPE = 'Launch Presentation Distributor Training'
         THEN 1 
            ELSE 0
        END) AS LAUNCH_PRESENTATION_DISTRIBUTOR,
        MAX(CASE 
            WHEN  DOC.DOC_TYPE  = 'Data Sheet'
         THEN 1 
            ELSE 0
        END) AS DATA_SHEET_AVAILABLE,
        COUNT(*)
    from
        EWPC_ORDERABLE_PART O_PART
    LEFT JOIN EWPC_PART_DOCUMENT_SEC PRODUCT_SEC 
        ON O_PART.BASE_PART = PRODUCT_SEC.BASE_PART
    LEFT JOIN EWPC_DOCUMENT_SEC DOC_SEC
        ON PRODUCT_SEC.DOC_ID = DOC_SEC.DOC_ID 
        AND DOC_SEC.WEB_VISIBLE='T'
        
    LEFT JOIN EWPC_PART_DOCUMENT PRODUCT
        ON O_PART.BASE_PART = PRODUCT.BASE_PART
    LEFT JOIN EWPC_DOCUMENT DOC
        ON PRODUCT.DOC_ID = DOC.DOC_ID 
        AND DOC.WEB_VISIBLE='T'

  GROUP BY O_PART.ORDERABLE_PART
),    

LKP_WEBPARTS AS
(
    SELECT 
        ORDR.ORDERABLE_PART as PRODUCT_ID,
        BASE.BASE_PART as WEB_PART_NAME,
        BASE.SHORT_DESC as WEB_PART_DESC,
        Case
            When BASE.web_visible = 'T' and ORDR.WEB_VISIBLE 
            Then 1 
            else 0 
         End AS Web_Visible_Flag
    FROM EWPC_BASE_PART BASE
    JOIN EWPC_ORDERABLE_PART ORDR
    On BASE.BASE_PART = ORDR.BASE_PART
    Where
        ORDR.PAL_CODE not like '#%'
        AND ORDR.PAL_CODE not like 'E%'
        AND ORDR.PAL_CODE not like 'U%'
        AND ORDR.PAL_CODE not like 'N%'
        AND ORDR.PAL_CODE not like 'Z%'
        AND ORDR.ORDERABLE_PART != 'NGTB50N65FL2WG'
        AND ORDR.ORDERABLE_PART != 'NGTB25N120FL2WG'
),

FINAL_SQL AS
(
    SELECT
    MD5(PRC_OWNR.MPN) AS PRODUCT_KEY,
    PRC_OWNR.MPN AS PRODUCT_ID,
    'ModelN' AS PRODUCT_SOURCE,
    PRC_OWNR.MULT AS MOQ_QUANTITY,
    PRC_OWNR.OID AS PART_OID_AK_ID,
    PRC_OWNR.CREATED_DATE,
    COALESCE(
        TRY_TO_BOOLEAN( 
                CASE 
                    WHEN LKP_WPRTS.WEB_VISIBLE_FLAG = 1 
                    THEN 'Y' 
                    ELSE 'N' 
                END
            )
        ,'FALSE'
    )::BOOLEAN  AS IS_WEB_VISIBLE,
    NVL(LKP_WPRTS.WEB_PART_NAME, 'None') AS WEB_PART_NAME,
    NVL(LKP_WPRTS.WEB_PART_DESC, 'None') AS WEB_PART_GROUP,
    COALESCE(PRD_ATTR.LAUNCH_SALES_TIPS,0)::BOOLEAN AS IS_LAUNCH_SALES_TIPS  ,    
    COALESCE(PRD_ATTR.LAUNCH_PRESENTATION_CUSTOMER,0)::BOOLEAN AS IS_LAUNCH_PRESENTATION_CUSTOMER ,
    COALESCE(PRD_ATTR.LAUNCH_PRESENTATION_DISTRIBUTOR,0)::BOOLEAN AS IS_LAUNCH_PRESENTATION_DISTRIBUTOR ,
    COALESCE(PRD_ATTR.DATA_SHEET_AVAILABLE,0)::BOOLEAN AS IS_DATA_SHEET_AVAILABLE ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} AS BIW_BATCH_ID
FROM PRICING_OWNER_PART PRC_OWNR
LEFT OUTER JOIN LKP_WEBPARTS LKP_WPRTS
    ON  PRC_OWNR.MPN=LKP_WPRTS.PRODUCT_ID
LEFT JOIN PRODUCT_DOC_TYPE PRD_ATTR   
    ON PRC_OWNR.MPN = PRD_ATTR.ORDERABLE_PART  
)

SELECT 
    *,
     MD5(
        OBJECT_CONSTRUCT ('col1',PRODUCT_KEY, 'col2',PRODUCT_ID, 'col3',PRODUCT_SOURCE, 'col5',IS_WEB_VISIBLE
        ,'col6',WEB_PART_NAME ,'col7',WEB_PART_GROUP,'col8',MOQ_QUANTITY,'col9',PART_OID_AK_ID,'col10',CREATED_DATE	
        ,'col11',IS_LAUNCH_SALES_TIPS ,'col12',IS_LAUNCH_PRESENTATION_CUSTOMER,'col13',IS_LAUNCH_PRESENTATION_DISTRIBUTOR,'col14',IS_DATA_SHEET_AVAILABLE)::string 
	) as BIW_MD5_KEY
FROM FINAL_SQL