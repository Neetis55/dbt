/*---------------------------------------------------------------------------
Command to run model:
-- dbt build --full-refresh --select ETL_MART_SALES_SAMPLE_FACT
-- dbt build --select ETL_MART_SALES_SAMPLE_FACT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         22 DEC 2022     KALI D              Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['SAMPLE_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_SAMPLE_FACT'-%}
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
         description = 'Building table SAMPLE_FACT for MART_SALES'
        ,transient=true
        ,materialized='view'
        ,schema ='ETL_MART_SALES'
        ,alias='SAMPLE_FACT'
		,unique_key= v_pk_list
        ,tags =['MART_SALES']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}


WITH SAMPLE_LINEITEM AS
(
    SELECT 
        ID, 
        ORDER_ID, 
        LINEITEM_ORDER, 
        CREATED, 
        MPN, 
        CREATED_BY, 
        SAMPLE_STATUS, 
        WORKFLOW_STATUS, 
        WORKFLOW_UPDATED, 
        DESIGN_CUSTOMER_NAME, 
        END_CUSTOMER_NAME, 
        DESCRIPTION, 
        QUANTITY, 
        SOURCE_TYPE, 
        CORP_CODE, 
        APPROVER_USERID, 
        APPROVER_NAME, 
        APPROVER_USERID_SEC, 
        SOURCE_OID, 
        SOURCE_NUMBER, 
        PRICE,
        SHIP_ADDR_ID
    FROM {{source('STG_DISTSAMPLESDB_PUBLIC','SAMPLE_LINEITEM')}}
    QUALIFY( ROW_NUMBER() OVER(PARTITION BY ORDER_ID,LINEITEM_ORDER  ORDER BY BIW_UPD_DTTM DESC )=1)
)

,SAMPLE_SHIPMENT AS
(
    SELECT 
        ID,
        ORDER_NUMBER, 
        LINE_ID, 
        SALES_ORDER_ID, 
        LINEITEM_ID,
        CSSOP_LINE_ITEM, 
        SHIP_DATE, 
        TRACKING_NUMBER, 
        QTY_ORDERED, 
        QTY_SHIPPED
    FROM {{source('STG_DISTSAMPLESDB_PUBLIC','SAMPLE_SHIPMENT')}}
    QUALIFY( ROW_NUMBER() OVER(PARTITION BY ID  ORDER BY BIW_UPD_DTTM DESC )=1)
)

,SAMPLE_ADDRESS AS
(
    SELECT 
        ID,
        ADDRESS1, 
        ADDRESS2, 
        ADDRESS3, 
        ADDRESS4, 
        ADDRESS5, 
        CITY, 
        COUNTRY, 
        PHONE, 
        EMAIL, 
        ZIP
    FROM {{source('STG_DISTSAMPLESDB_PUBLIC','SAMPLE_ADDRESS')}}
    QUALIFY( ROW_NUMBER() OVER(PARTITION BY ID  ORDER BY BIW_UPD_DTTM DESC )=1)
)

,SAMPLE_USER AS
(
    SELECT 
        USERID,
        NAME, 
        SURNAME, 
        COMPANY, 
        EMAIL, 
        USER_CHANNEL
    FROM {{source('STG_DISTSAMPLESDB_PUBLIC','SAMPLE_USER')}}
    QUALIFY( ROW_NUMBER() OVER(PARTITION BY USERID ORDER BY BIW_UPD_DTTM DESC )=1)
)


SELECT 
    MD5(OBJECT_CONSTRUCT (  'COL1',LN.ORDER_ID::STRING
                            ,'COL2',LN.LINEITEM_ORDER::STRING
                            ,'COL3',SHIP.LINE_ID::STRING
                         )::STRING 
        )::BINARY AS SAMPLE_KEY, 
    REPLACE(SUBSTRING(LN.CREATED,1,10),'-')::NUMBER  AS DATE_KEY,
    MD5(LN.MPN::STRING) AS MARKET_PRODUCT_NUMBER_KEY, 
    LN.ID AS SAMPLE_LINE_ITEM_ID, 
    LN.ORDER_ID, 
    LN.LINEITEM_ORDER AS LINE_ITEM_ID, 
    SHIP.LINE_ID AS SHIPMENT_ID, 
    LN.CREATED AS SAMPLE_CREATED_DATE,
    LN.MPN AS MARKET_PRODUCT_NUMBER, 
    LN.CREATED_BY AS LINE_ITEM_CREATED_BY, 
    LN.SAMPLE_STATUS AS LINE_ITEM_STATUS, 
    LN.WORKFLOW_STATUS AS LINE_ITEM_WORKFLOW_STATUS, 
    LN.WORKFLOW_UPDATED AS LINE_ITEM_WORKFLOW_UPDATED_DATE, 
    USR.NAME||' '||USR.SURNAME AS SAMPLE_CREATED_USER,  
    USR.COMPANY AS SAMPLE_USER_COMPANY, 
    LN.DESIGN_CUSTOMER_NAME AS LINE_ITEM_DESIGN_CUSTOMER_NAME, 
    LN.END_CUSTOMER_NAME, 
    LN.DESCRIPTION AS LINE_ITEM_DESCRIPTION, 
    USR.NAME||' '||USR.SURNAME  AS SAMPLE_CREATED_BY, 
    USR.EMAIL AS SAMPLE_CREATED_BY_EMAIL, 
    USR.USER_CHANNEL AS SAMPLE_USER_CHANNEL, 
    LN.QUANTITY AS SAMPLE_REQUESTED_QUANTITY, 
    LN.SOURCE_TYPE, 
    LN.CORP_CODE AS CORPORATION_CODE, 
    LN.APPROVER_USERID AS LINE_ITEM_APPROVER_USER_ID, 
    LN.APPROVER_NAME AS LINE_ITEM_APPROVER_NAME, 
    LN.APPROVER_USERID_SEC AS SECONDARY_LINE_ITEM_APPROVER_USER_ID, 
    LN.SOURCE_OID AS LINE_ITEM_SOURCE_OID, 
    ADDR.ADDRESS1 AS SAMPLE_SHIP_TO_CONTACT, 
    ADDR.ADDRESS2 AS SAMPLE_SHIP_TO_COMPANY, 
    ADDR.ADDRESS3 AS SAMPLE_SHIP_TO_ADDRESS1, 
    ADDR.ADDRESS4 AS SAMPLE_SHIP_TO_ADDRESS2, 
    ADDR.ADDRESS5 AS SAMPLE_SHIP_TO_ADDRESS3, 
    ADDR.CITY AS SAMPLE_SHIP_TO_CITY, 
    ADDR.COUNTRY AS SAMPLE_SHIP_TO_COUNTRY, 
    ADDR.PHONE AS SAMPLE_SHIP_TO_CONTACT_PHONE, 
    ADDR.EMAIL AS SAMPLE_SHIP_TO_CONTACT_EMAIL, 
    ADDR.ZIP AS SAMPLE_SHIP_TO_ZIP, 
    LN.SOURCE_NUMBER AS LINE_ITEM_OPPORTUNITY_NUMBER, 
    SHIP.ORDER_NUMBER AS SAMPLE_SHIPMENT_ORDER_NUMBER, 
    SHIP.SALES_ORDER_ID, 
    SHIP.CSSOP_LINE_ITEM AS SAMPLE_CASSOP_LINE_ITEM, 
    SHIP.SHIP_DATE AS SAMPLE_SHIP_DATE, 
    SHIP.TRACKING_NUMBER AS SAMPLE_SHIPMENT_TRACKING_NUMBER, 
    SHIP.QTY_ORDERED AS SAMPLE_QUANTITY_ORDERED, 
    SHIP.QTY_SHIPPED AS SAMPLE_QUANTITY_SHIPPED, 
    LN.PRICE AS SAMPLE_PRICE_USD, 
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 AS BIW_BATCH_ID,
    md5(object_construct ('col1',DATE_KEY::string, 'col2',MARKET_PRODUCT_NUMBER_KEY::string, 'col3',SAMPLE_LINE_ITEM_ID::string, 'col4',ORDER_ID::string,
    'col5',LINE_ITEM_ID::string, 'col6',SHIPMENT_ID::string, 'col7',SAMPLE_CREATED_DATE::string, 'col8',MARKET_PRODUCT_NUMBER::string,
    'col9',LINE_ITEM_CREATED_BY::string, 'col10',LINE_ITEM_STATUS::string, 'col11',LINE_ITEM_WORKFLOW_STATUS::string,
    'col12',LINE_ITEM_WORKFLOW_UPDATED_DATE::string, 'col13',SAMPLE_CREATED_USER::string, 'col14',SAMPLE_USER_COMPANY::string,
    'col15',LINE_ITEM_DESIGN_CUSTOMER_NAME::string, 'col16',END_CUSTOMER_NAME::string, 'col17',LINE_ITEM_DESCRIPTION::string,
    'col18',SAMPLE_CREATED_BY::string, 'col19',SAMPLE_CREATED_BY_EMAIL::string, 'col20',SAMPLE_USER_CHANNEL::string,
    'col21',SAMPLE_REQUESTED_QUANTITY::string, 'col22',SOURCE_TYPE::string, 'col23',CORPORATION_CODE::string,
    'col24',LINE_ITEM_APPROVER_USER_ID::string, 'col25',LINE_ITEM_APPROVER_NAME::string, 'col26',SECONDARY_LINE_ITEM_APPROVER_USER_ID::string,
    'col27',LINE_ITEM_SOURCE_OID::string, 'col28',SAMPLE_SHIP_TO_CONTACT::string, 'col29',SAMPLE_SHIP_TO_COMPANY::string,
    'col30',SAMPLE_SHIP_TO_ADDRESS1::string, 'col31',SAMPLE_SHIP_TO_ADDRESS2::string, 'col32',SAMPLE_SHIP_TO_ADDRESS3::string,
    'col33',SAMPLE_SHIP_TO_CITY::string, 'col34',SAMPLE_SHIP_TO_COUNTRY::string, 'col35',SAMPLE_SHIP_TO_CONTACT_PHONE::string,
    'col36',SAMPLE_SHIP_TO_CONTACT_EMAIL::string, 'col37',SAMPLE_SHIP_TO_ZIP::string, 'col38',LINE_ITEM_OPPORTUNITY_NUMBER::string,
    'col39',SAMPLE_SHIPMENT_ORDER_NUMBER::string, 'col40',SALES_ORDER_ID::string, 'col41',SAMPLE_CASSOP_LINE_ITEM::string,
    'col42',SAMPLE_SHIP_DATE::string, 'col43',SAMPLE_SHIPMENT_TRACKING_NUMBER::string, 'col44',SAMPLE_QUANTITY_ORDERED::string,
    'col45',SAMPLE_QUANTITY_SHIPPED::string, 'col46',SAMPLE_PRICE_USD::string)::string )::BINARY as BIW_MD5_KEY
FROM SAMPLE_LINEITEM LN
LEFT JOIN SAMPLE_ADDRESS ADDR
    ON LN.SHIP_ADDR_ID = ADDR.ID 
LEFT JOIN SAMPLE_SHIPMENT SHIP 
    ON LN.ID = SHIP.LINEITEM_ID
LEFT JOIN SAMPLE_USER USR
    ON LN.CREATED_BY = USR.USERID