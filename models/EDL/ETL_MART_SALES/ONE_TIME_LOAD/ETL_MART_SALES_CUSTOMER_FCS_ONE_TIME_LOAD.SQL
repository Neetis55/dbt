/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_SALES_CUSTOMER_FCS_ONE_TIME_LOAD

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         20 OCT 2022      VINAYAKA CN    Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['CUSTOMER_KEY']-%}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_CUSTOMER_FCS_ONE_TIME_LOAD'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{%- set V_EDW_DB = var('V_EDW_DEFAULT_DB') +env_var('DBT_DEP_ENV')+'.' -%}


{################# Snowflake Object Configuration #################}
{{
    config(
         description = 'Building ETL VIEW CUSTOMER Fact Historical DWH customer CUSTOMER_FCS_ONE_TIME_LOAD '
        ,transient=true
        ,materialized='view'
        ,schema ='ETL_MART_SALES'
        ,alias='CUSTOMER_FCS_ONE_TIME_LOAD'
        ,tags =['MART_SALES']
        )
}}

WITH FCS_CUSTOMER AS (
    select 
        CUST_CD AS CUSTOMER_CODE ,
        ERP_SITEUSE_ID AS ERP_SITE_USE_ID ,
        ERP_STATUS AS ERP_STATUS ,
        CRM_CUSTOMER_OID AS CRM_CUSTOMER_OID ,
        CRM_STATUS AS CRM_STATUS ,
        CUST_NAME AS CUSTOMER_NAME ,
        CUST_TYPE AS CUSTOMER_TYPE_CODE ,
        CUST_TYPE_DESC AS CUSTOMER_TYPE_DESCRIPTION ,
        CUST_SOURCE AS CUSTOMER_SOURCE ,
        BUSINESS_CLASS_CD AS BUSINESS_CLASS_CODE ,
        CORP_CD AS CORPORATION_CODE ,
        CORP_DESC AS CORPORATION_DESCRIPTION ,
        END_CORP_CD AS END_CORPORATION_CODE ,
        END_CORP_DESC AS END_CORPORATION_DESCRIPTION ,
        SALES_ORG_CD AS SALES_ORGANIZATION_CODE ,
        END_USE_CD AS END_USE_CODE ,
        END_USE_DESC AS END_USE_DESCRIPTION ,
        CURRENCY_CD AS CURRENCY_CODE ,
        CREDIT_HOLD_FLAG AS HAS_CREDIT_HOLD ,
        SERVICE_HOLD_FLAG AS HAS_SERVICE_HOLD ,
        ORDRPROCREGION_CD AS ORDER_PROCESSING_REGION_CODE ,
        CRD_SHPERLY_DY_QTY AS CRD_SHIP_EARLY_DAYS_QUANTITY ,
        CRD_PARTIAL_FLAG AS HAS_CRD_PARTIAL ,
        SHIP_VIA_CD AS SHIP_VIA_CODE ,
        SHIP_TO_CRD_FLAG AS HAS_SHIP_TO_CRD ,
        SHIP_LATE_DAYS_QTY AS SHIP_LATE_DAYS_QUANTITY ,
        BILL_CUST_CD AS BILL_CUSTOMER_CODE ,
        HANDLING_METHOD_CD AS HANDLING_METHOD_CODE ,
        HANDLING_METHOD_CD_DESC AS HANDLING_METHOD_CODE_DESCRIPTION ,
        EDI_CUSTOMER_FLAG AS IS_EDI_CUSTOMER ,
        FINAL_DEST_DESC AS FINAL_DESTINATION_DESCRIPTION ,
        BILL_CNTRY_SRCE_CD AS BILL_COUNTRY_SOURCE_CODE ,
        FOB_OPTION_CD AS FOB_OPTION_CODE ,
        PAYMEMT_TERM_CD AS PAYMENT_TERM_CODE ,
        TRANSIT_DAYS_QTY AS TRANSIT_DAYS_QUANTITY ,
        DELIVERY_TYPE_CD AS DELIVERY_TYPE_CODE ,
        DATE_CODE_MAX_AGE_QTY AS DATE_CODE_MAXIMUM_AGE_QUANTITY ,
        CPN_REQUIRED_FLAG AS IS_CPN_REQUIRED ,
        VAT_TAX_CD AS VAT_TAX_CODE ,
        CUSTOMER_ENTITY_CD AS CUSTOMER_ENTITY_CODE ,
        FORECAST_TYPE_CD AS FORECAST_TYPE_CODE ,
        FORECAST_CUSTOMER_FLAG AS IS_FORECAST_CUSTOMER ,
        REPLENISHMENT_CODE AS REPLENISHMENT_CODE ,
        UDC_GENERIC_PROG_DESC AS UDC_GENERIC_PROG_DESCRIPTION ,
        CREATED_DATE AS CREATE_DTTM ,
        'Y' AS IS_DELETED ,
        QUALITY_CD AS QUALITY_CODE ,
        FISCAL_ACCOUNT_NUM AS FISCAL_ACCOUNT_NUMBER ,
        CREDIT_LIMIT_AMT AS CREDIT_LIMIT_AMOUNT ,
        SALES_STRUCTURE AS SALES_STRUCTURE ,
        ASSEMBLY_LOCATION_FLAG AS IS_ASSEMBLY_LOCATION ,
        CUST_SHUT_DOWN_FROM_DATE AS CUSTOMER_SHUT_DOWN_FROM_DATE ,
        CUST_SHUT_DOWN_TO_DATE AS CUSTOMER_SHUT_DOWN_TO_DATE ,
        CSR_USERID AS CSR_USER_ID ,
        FSE_USERID AS FSE_USER_ID ,
        APPROVAL_CD AS APPROVAL_CODE,
        FINCL_BANK_ID AS FINANCIAL_BANK_ID,
        CATEGORY AS CORPORATION_CATEGORY_CODE,
        CATEGORY_DESC AS CORPORATION_CATEGORY_DESCRIPTION,
        UPPER(FORECAST_CUSTOMER_STATUS) AS FORECAST_CUSTOMER_STATUS_CODE ,
        SERVICE_TYPE AS SERVICE_TYPE_CODE ,
        UPPER(CORPORATION_DIVISION_DESC) AS DIVISION ,
        ORACLE_STATUS AS ORACLE_SITE_STATUS,
        KEY_MARKETSEGMENT
    FROM 
        {{ source('STG_SDM_PRESENTATION', 'VW_CUSTOMERS') }}   
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY CUST_CD ORDER BY BIW_UPD_DTTM DESC)=1)
),
CONTACTS_DIM AS (
    SELECT 
        CUST_CD AS CUSTOMER_CODE,
        ROLE ,
        USER_ID ,
        FIRST_NM AS FIRST_NAME ,
        LAST_NM AS LAST_NAME ,
        EMAIL ,
        PHONE_DESC AS PHONE ,
        LAST_UPD_BY AS CONTACT_LAST_UPDATE_BY ,
        CREATED_BY AS CONTACT_CREATED_BY 
    FROM 
        {{ source('STG_DWH_MARTS', 'CUST_CONTACTS_DIM') }}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY CUST_CD,ROLE ORDER BY BIW_UPD_DTTM DESC)=1)   
),
CURRENCY AS (
    SELECT  CURRENCY_CD AS CURRENCY_CODE,
            ISO_CURRENCY_CD AS ISO_CURRENCY_CODE
    FROM 
        {{ source('STG_DWH_MARTS', 'CUSTOMER_DIM') }}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY CURRENCY_CD,ISO_CURRENCY_CD ORDER BY BIW_UPD_DTTM DESC)=1)   
),
FOB_LKP AS (
    SELECT  FOB_OPTION_CD AS FOB_OPTION_CODE,
            FOB_OPTION_DESC AS FOB_OPTION_DESCRIPTION
    FROM 
        {{ source('STG_DWH_MARTS', 'CUSTOMER_DIM') }}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY FOB_OPTION_CD ORDER BY BIW_UPD_DTTM DESC)=1)   
),
CORP_LKP AS (
    SELECT 
        CORP_CD AS CORPORATION_CODE ,
        CORP_CATEGORY_CD AS CORPORATION_CATEGORY_CODE,
        CORP_CATEGORY_DESC AS CORPORATION_CATEGORY_DESCRIPTION
    FROM 
        {{ source('STG_SDM_PRESENTATION', 'LKP_CORPORATIONS') }}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY CORP_CD ORDER BY BIW_UPD_DTTM DESC)=1)
),
FINAL_SQL AS (
    SELECT 
    ----------------------FK KEY------------------------------
        MD5(CUST.CUSTOMER_CODE) AS CUSTOMER_KEY ,
    ----------------------FK CODE------------------------------
        CUST.CUSTOMER_CODE ,
        CUST.CUSTOMER_TYPE_CODE ,
        CUST.BUSINESS_CLASS_CODE ,
        CUST.CORPORATION_CODE ,
        CUST.END_CORPORATION_CODE ,
        CUST.SALES_ORGANIZATION_CODE ,
    --------------------OTHER CODE------------------------
        CUST.END_USE_CODE ,
        CUST.CURRENCY_CODE ,
        CUR.ISO_CURRENCY_CODE ,
        CUST.ORDER_PROCESSING_REGION_CODE ,
        CUST.SHIP_VIA_CODE ,
        CUST.BILL_CUSTOMER_CODE ,
        CUST.DELIVERY_TYPE_CODE ,
        CUST.FOB_OPTION_CODE ,
        CUST.PAYMENT_TERM_CODE ,
        CUST.VAT_TAX_CODE ,
        CUST.CUSTOMER_ENTITY_CODE ,
        CUST.FORECAST_TYPE_CODE ,
    --------------------FLAG COLUMNS------------------------
        COALESCE(CUST.HAS_CREDIT_HOLD,FALSE)::BOOLEAN AS HAS_CREDIT_HOLD,
        COALESCE(CUST.HAS_SERVICE_HOLD,FALSE)::BOOLEAN AS HAS_SERVICE_HOLD,
        COALESCE(
            CASE 
            WHEN APPROVAL_CODE='Y' THEN TRUE
            WHEN APPROVAL_CODE IN ('X','N') THEN FALSE 
            ELSE NULL 
        END,FALSE)::BOOLEAN AS HAS_APPROVAL ,
        COALESCE(CUST.HAS_CRD_PARTIAL,FALSE)::BOOLEAN AS HAS_CRD_PARTIAL,
        COALESCE(CUST.HAS_SHIP_TO_CRD,FALSE)::BOOLEAN AS HAS_SHIP_TO_CRD,
        COALESCE(CUST.IS_DELETED,FALSE)::BOOLEAN AS IS_DELETED,
        COALESCE(CUST.IS_EDI_CUSTOMER,FALSE)::BOOLEAN AS IS_EDI_CUSTOMER,
        COALESCE(CUST.IS_CPN_REQUIRED,FALSE)::BOOLEAN AS IS_CPN_REQUIRED,
        NULL::BOOLEAN AS IS_MINIMUM_DOLLAR_VALUE_VALID,
        COALESCE(CUST.IS_FORECAST_CUSTOMER,FALSE)::BOOLEAN AS IS_FORECAST_CUSTOMER,
        COALESCE(CUST.IS_ASSEMBLY_LOCATION,FALSE)::BOOLEAN AS IS_ASSEMBLY_LOCATION,
        NULL::BOOLEAN AS IS_SHUTDOWN,
    ---------------------OTHER ATTRIBUTES--------------------
        CUST.ERP_SITE_USE_ID ,
        CUST.ERP_STATUS ,
        CUST.CRM_CUSTOMER_OID ,
        CUST.CRM_STATUS ,
        CUST.CUSTOMER_NAME ,
        CUST.CUSTOMER_TYPE_DESCRIPTION ,
        COALESCE(CUST.CUSTOMER_SOURCE,'HISTORY_FAIRCHILD') AS CUSTOMER_SOURCE ,
        CUST.CORPORATION_DESCRIPTION ,
        CUST.END_USE_DESCRIPTION ,
        CUST.END_CORPORATION_DESCRIPTION ,
        CUST.CRD_SHIP_EARLY_DAYS_QUANTITY ,
        CUST.SHIP_LATE_DAYS_QUANTITY ,
        CUST.HANDLING_METHOD_CODE ,
        CUST.HANDLING_METHOD_CODE_DESCRIPTION ,
        CUST.FINAL_DESTINATION_DESCRIPTION ,
        CUST.BILL_COUNTRY_SOURCE_CODE ,
        CUST.FINANCIAL_BANK_ID ,
        CUST.TRANSIT_DAYS_QUANTITY ,   
        CUST.DATE_CODE_MAXIMUM_AGE_QUANTITY ,
        NULL AS MINIMUM_DOLLAR_VALUE_AMOUNT ,
        CORP.CORPORATION_CATEGORY_CODE ,
        CORP.CORPORATION_CATEGORY_DESCRIPTION ,
        CUST.FORECAST_CUSTOMER_STATUS_CODE ,
        CUST.REPLENISHMENT_CODE ,
        CUST.SERVICE_TYPE_CODE ,
        NULL AS CHANGE_ORDER_APROVAL_REQUIRED_CODE ,
        CUST.UDC_GENERIC_PROG_DESCRIPTION ,
        NULL AS CREATE_BY_ID ,
        CUST.CREATE_DTTM ,
        NULL AS LAST_UPDATE_BY_ID ,
        NULL AS LAST_UPDATE_DTTM , 
        CUST.QUALITY_CODE ,
        CUST.FISCAL_ACCOUNT_NUMBER ,
        CUST.CREDIT_LIMIT_AMOUNT ,
        CUST.DIVISION ,
        CUST.SALES_STRUCTURE ,
        NULL AS MARKET_SEGMENT_CODE ,     
        NULL AS OPERATING_UNIT ,
        NULL AS COF_CODE ,
        NULL AS COG_CODE ,
        CUST.CUSTOMER_SHUT_DOWN_FROM_DATE ,
        CUST.CUSTOMER_SHUT_DOWN_TO_DATE ,
        FOB.FOB_OPTION_DESCRIPTION ,
        NULL AS TAX_CODE ,
        NULL AS ORACLE_ORDER_TYPE ,
        NULL AS ORACLE_PAYMENT_TERM ,
        CUST.ORACLE_SITE_STATUS ,
        NULL AS ORACLE_SITE_NUMBER ,
        NULL AS ORACLE_SHIP_METHOD ,
        NULL AS FREE_TRADE_ZONE ,
        NULL AS COLLECTOR_NAME ,
        ----------------------- CSR -----------------------------
        CUST.CSR_USER_ID ,
        CSR.FIRST_NAME              AS CSR_FIRST_NAME ,
        CSR.LAST_NAME               AS CSR_LAST_NAME ,
        CSR.EMAIL                   AS CSR_EMAIL ,
        CSR.PHONE                   AS CSR_PHONE ,
        CSR.CONTACT_LAST_UPDATE_BY  AS CSR_CONTACT_LAST_UPDATE_BY ,
        CSR.CONTACT_CREATED_BY      AS CSR_CONTACT_CREATED_BY ,
        ----------------------- FAQ -----------------------------
        FAQ.USER_ID                 AS FQA_USER_ID ,
        FAQ.FIRST_NAME              AS FQA_FIRST_NAME ,
        FAQ.LAST_NAME               AS FQA_LAST_NAME ,
        FAQ.EMAIL                   AS FQA_EMAIL ,
        FAQ.PHONE                   AS FQA_PHONE ,
        FAQ.CONTACT_LAST_UPDATE_BY  AS FQA_CONTACT_LAST_UPDATE_BY ,
        FAQ.CONTACT_CREATED_BY      AS FQA_CONTACT_CREATED_BY ,
        ----------------------- BUYER -----------------------------
        BUY.USER_ID                 AS BUYER_USER_ID ,
        BUY.FIRST_NAME              AS BUYER_FIRST_NAME ,
        BUY.LAST_NAME               AS BUYER_LAST_NAME ,
        BUY.EMAIL                   AS BUYER_EMAIL ,
        BUY.PHONE                   AS BUYER_PHONE ,
        BUY.CONTACT_LAST_UPDATE_BY  AS BUYER_CONTACT_LAST_UPDATE_BY ,
        BUY.CONTACT_CREATED_BY      AS BUYER_CONTACT_CREATED_BY ,
        ----------------------- FSE -----------------------------
        CUST.FSE_USER_ID ,
        FSE.FIRST_NAME              AS FSE_FIRST_NAME ,
        FSE.LAST_NAME               AS FSE_LAST_NAME ,
        FSE.EMAIL                   AS FSE_EMAIL ,
        FSE.PHONE                   AS FSE_PHONE ,
        FSE.CONTACT_LAST_UPDATE_BY  AS FSE_CONTACT_LAST_UPDATE_BY ,
        FSE.CONTACT_CREATED_BY      AS FSE_CONTACT_CREATED_BY ,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
        {{V_BIW_BATCH_ID}} as BIW_BATCH_ID
    
    FROM FCS_CUSTOMER AS CUST
    LEFT JOIN CURRENCY AS CUR 
        ON    CUST.CURRENCY_CODE = CUR.CURRENCY_CODE
    LEFT JOIN FOB_LKP AS FOB
        ON     CUST.FOB_OPTION_CODE = FOB.FOB_OPTION_CODE       
    LEFT JOIN CORP_LKP AS CORP 
        ON   CUST.CORPORATION_CODE = CORP.CORPORATION_CODE
    LEFT JOIN CONTACTS_DIM AS CSR 
        ON CUST.CUSTOMER_CODE = CSR.CUSTOMER_CODE     
        AND CSR.ROLE= 'CSR'
    LEFT JOIN CONTACTS_DIM AS FAQ 
        ON CUST.CUSTOMER_CODE = FAQ.CUSTOMER_CODE     
        AND FAQ.ROLE= 'FQA'
    LEFT JOIN CONTACTS_DIM AS BUY 
        ON CUST.CUSTOMER_CODE = BUY.CUSTOMER_CODE     
        AND BUY.ROLE= 'BUYER'
    LEFT JOIN CONTACTS_DIM AS FSE 
        ON CUST.CUSTOMER_CODE = FSE.CUSTOMER_CODE     
        AND FSE.ROLE= 'FSE'
                         
)

SELECT 
    DISTINCT FIN.*
    ,MD5(object_construct ('col1',CUSTOMER_KEY::string, 'col2',CUSTOMER_TYPE_CODE::string, 'col3',BUSINESS_CLASS_CODE::string, 'col4',CORPORATION_CODE::string, 'col5',END_CORPORATION_CODE::string
        , 'col6',SALES_ORGANIZATION_CODE::string, 'col7',END_USE_CODE::string, 'col8',CURRENCY_CODE::string, 'col9',ISO_CURRENCY_CODE::string, 'col10',ORDER_PROCESSING_REGION_CODE::string
        , 'col11',SHIP_VIA_CODE::string, 'col12',BILL_CUSTOMER_CODE::string, 'col13',DELIVERY_TYPE_CODE::string, 'col14',FOB_OPTION_CODE::string, 'col15',PAYMENT_TERM_CODE::string
        , 'col16',VAT_TAX_CODE::string, 'col17',CUSTOMER_ENTITY_CODE::string, 'col18',FORECAST_TYPE_CODE::string, 'col19',HAS_CREDIT_HOLD::string, 'col20',HAS_SERVICE_HOLD::string
        , 'col21',HAS_APPROVAL::string, 'col22',HAS_CRD_PARTIAL::string, 'col23',HAS_SHIP_TO_CRD::string, 'col24',IS_DELETED::string, 'col25',IS_EDI_CUSTOMER::string
        , 'col26',IS_CPN_REQUIRED::string, 'col27',IS_MINIMUM_DOLLAR_VALUE_VALID::string, 'col28',IS_FORECAST_CUSTOMER::string, 'col29',IS_ASSEMBLY_LOCATION::string, 'col30',IS_SHUTDOWN::string
        , 'col31',ERP_SITE_USE_ID::string, 'col32',ERP_STATUS::string, 'col33',CRM_CUSTOMER_OID::string, 'col34',CRM_STATUS::string, 'col35',CUSTOMER_NAME::string
        , 'col36',CUSTOMER_TYPE_DESCRIPTION::string, 'col37',CUSTOMER_SOURCE::string, 'col38',CORPORATION_DESCRIPTION::string, 'col39',END_USE_DESCRIPTION::string, 'col40',END_CORPORATION_DESCRIPTION::string
        , 'col41',CRD_SHIP_EARLY_DAYS_QUANTITY::string, 'col42',SHIP_LATE_DAYS_QUANTITY::string, 'col43',HANDLING_METHOD_CODE::string, 'col44',HANDLING_METHOD_CODE_DESCRIPTION::string, 'col45',FINAL_DESTINATION_DESCRIPTION::string
        , 'col46',BILL_COUNTRY_SOURCE_CODE::string, 'col47',FINANCIAL_BANK_ID::string, 'col48',TRANSIT_DAYS_QUANTITY::string, 'col49',DATE_CODE_MAXIMUM_AGE_QUANTITY::string, 'col50',MINIMUM_DOLLAR_VALUE_AMOUNT::string
        , 'col51',CORPORATION_CATEGORY_CODE::string, 'col52',CORPORATION_CATEGORY_DESCRIPTION::string, 'col53',FORECAST_CUSTOMER_STATUS_CODE::string, 'col54',REPLENISHMENT_CODE::string, 'col55',SERVICE_TYPE_CODE::string
        , 'col56',CHANGE_ORDER_APROVAL_REQUIRED_CODE::string, 'col57',UDC_GENERIC_PROG_DESCRIPTION::string, 'col58',CREATE_BY_ID::string, 'col59',CREATE_DTTM::string, 'col60',LAST_UPDATE_BY_ID::string
        , 'col61',LAST_UPDATE_DTTM::string, 'col62',QUALITY_CODE::string, 'col63',FISCAL_ACCOUNT_NUMBER::string, 'col64',CREDIT_LIMIT_AMOUNT::string, 'col65',DIVISION::string
        , 'col66',SALES_STRUCTURE::string, 'col67',MARKET_SEGMENT_CODE::string, 'col68',OPERATING_UNIT::string, 'col69',COF_CODE::string, 'col70',COG_CODE::string
        , 'col71',CUSTOMER_SHUT_DOWN_FROM_DATE::string, 'col72',CUSTOMER_SHUT_DOWN_TO_DATE::string, 'col73',FOB_OPTION_DESCRIPTION::string, 'col74',TAX_CODE::string, 'col75',ORACLE_ORDER_TYPE::string
        , 'col76',ORACLE_PAYMENT_TERM::string, 'col77',ORACLE_SITE_STATUS::string, 'col78',ORACLE_SITE_NUMBER::string, 'col79',ORACLE_SHIP_METHOD::string, 'col80',FREE_TRADE_ZONE::string
        , 'col81',COLLECTOR_NAME::string, 'col82',CSR_USER_ID::string, 'col83',CSR_FIRST_NAME::string, 'col84',CSR_LAST_NAME::string, 'col85',CSR_EMAIL::string
        , 'col86',CSR_PHONE::string, 'col87',CSR_CONTACT_LAST_UPDATE_BY::string, 'col88',CSR_CONTACT_CREATED_BY::string, 'col89',FQA_USER_ID::string, 'col90',FQA_FIRST_NAME::string
        , 'col91',FQA_LAST_NAME::string, 'col92',FQA_EMAIL::string, 'col93',FQA_PHONE::string, 'col94',FQA_CONTACT_LAST_UPDATE_BY::string, 'col95',FQA_CONTACT_CREATED_BY::string
        , 'col96',BUYER_USER_ID::string, 'col97',BUYER_FIRST_NAME::string, 'col98',BUYER_LAST_NAME::string, 'col99',BUYER_EMAIL::string, 'col100',BUYER_PHONE::string
        , 'col101',BUYER_CONTACT_LAST_UPDATE_BY::string, 'col102',BUYER_CONTACT_CREATED_BY::string, 'col103',FSE_USER_ID::string, 'col104',FSE_FIRST_NAME::string, 'col105',FSE_LAST_NAME::string
        , 'col106',FSE_EMAIL::string, 'col107',FSE_PHONE::string, 'col108',FSE_CONTACT_LAST_UPDATE_BY::string, 'col109',FSE_CONTACT_CREATED_BY::string)::string )::BINARY AS BIW_MD5_KEY
FROM    
    FINAL_SQL as FIN