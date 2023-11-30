/*---------------------------------------------------------------------------
Command to run model:
--dbt build --select ETL_MART_SALES_PRODUCT_PRICE_WORKSHEET_HIST --vars 'is_backfill: True'
--dbt build --full-refresh --select ETL_MART_SALES_PRODUCT_PRICE_WORKSHEET_HIST
Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         07 Nov 2022      VINAYAKA CN       Initial Version
1.1         15 Nov 2022      Kali Dandapani    Updated logic for COGS price & Distibutor_BOOK_PRICE_JPY/EUR & "MODELN" Data source
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['GPS_KEY']-%}   
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_PRODUCT_PRICE_WORKSHEET_HIST'-%}
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
         description = 'Building table PRODUCT_PRICE_WORKSHEET_HIST for MART_SALES'
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_SALES'
        ,alias='PRODUCT_PRICE_WORKSHEET_HIST'
		,unique_key= v_pk_list
        ,tags =['MART_SALES']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}


WITH 
GPS_PRICE_OUT AS (
    SELECT
        ONPN                    AS PRODUCT_ID,
        EFFECTIVE_DT            AS EFFECTIVE_FROM_DATE ,
        COALESCE(LEAD(EFFECTIVE_DT) OVER(PARTITION BY ONPN ORDER BY EFFECTIVE_DT  ) -1,'9999-12-31') EFFECTIVE_TO_DATE,
        MD5(COALESCE(ONPN::STRING,'-1'))::BINARY AS PRODUCT_KEY , 
        CASE 
            WHEN DELETE_FLG = 'Y' 
                THEN TRUE
            WHEN DELETE_FLG = 'N' 
                THEN FALSE
            ELSE DELETE_FLG
        END::BOOLEAN            AS IS_DELETE ,
        DBP_PRICE               AS DISTRIBUTOR_BOOK_PRICE_USD ,
        EP_PRICE                AS END_POINT_PRICE_USD ,
        EP_QTY                  AS FIELD_MINIMUM_QUANTITY ,
        MPQ_QTY                 AS MINIMUM_PACKAGE_QUANTITY ,
        RESALE_PRICE_FACTOR     AS MULTIPLIER ,
        REF_PRICE               AS REFERENCE_PRICE ,
        SM_PRICE                AS STRATEGIC_MINIMUM_PRICE ,
        WALK_AWAY_PRICE         AS WALK_AWAY_PRICE ,
        TRIM(FRAME_NAME)        AS FRAME_NAME ,
        PRICE_CATENAME          AS GPS_PRICE_CATEGORY ,
        CATEGORY_CD             AS GPS_CATEGORY ,
        CASE 
            WHEN GPS_CATEGORY='A'  
                THEN 'PB Device'
            WHEN GPS_CATEGORY ='B' 
                THEN 'Device in PB and MPP'
            WHEN GPS_CATEGORY ='C' 
                THEN 'Device not in PB'
            ELSE GPS_CATEGORY 
        END                     AS GPS_CATEGORY_DESCRIPTION ,
        MARKET_CD               AS GPS_MARKET ,
        CASE 
            WHEN GPS_MARKET='C'     
                THEN 'Commodity'
            WHEN GPS_MARKET = 'E'   
                THEN 'External' 
            WHEN GPS_MARKET='L'     
                THEN 'Lifetime buy'
            WHEN GPS_MARKET = 'F' 
                THEN 'Firm Price' 
            WHEN GPS_MARKET = 'P' 
                THEN 'private(special)'                    
            WHEN GPS_MARKET='S'     
                THEN 'Sole source'
            WHEN GPS_MARKET = 'Q' 
                THEN 'Marketing Specials' 
            ELSE GPS_MARKET 
        END  AS GPS_MARKET_DESCRIPTION ,
        DECODE(PROGRAM_CD 
                , null,'#'
                , '','#' 
                , PROGRAM_CD
        )                       AS GPS_PROGRAM_CODE ,
        CASE 
            WHEN PRICE_BOOK_FLG = 'N' 
                THEN 'Not in Price Book'
            ELSE  
                CASE 
                    WHEN GPS_PROGRAM_CODE = 'M' 
                        THEN 'MPP'
                    WHEN GPS_PROGRAM_CODE = 'L' 
                        THEN 'Low Volume'
                    WHEN GPS_PROGRAM_CODE = 'G' 
                        THEN 'On Target'
                    ELSE 'No Program'
                END
        END                     AS GPS_PROGRAM_DESCRIPTION ,        
        CASE 
            WHEN GPS_PROGRAM_CODE = 'G' 
                THEN 'ON-TARGET'
            WHEN GPS_PROGRAM_CODE = 'M' 
                THEN 'Market price program' 
            WHEN GPS_PROGRAM_CODE = 'L' 
                THEN 'LoVIT program'
            WHEN GPS_PROGRAM_CODE = 'B' 
                THEN 'obsolete' 
            ELSE GPS_PROGRAM_CODE 
        END                     AS GPS_PROGRAM_CODE_DESCRIPTION ,
        MARGIN_LEVEL            AS MARGIN_LEVEL ,
        FRAME_TYPE              AS PRODUCT_FRAME_TYPE,
         CASE 
            WHEN FRAME_TYPE='I'   
                THEN 'Independent'
            WHEN FRAME_TYPE = 'C' 
                THEN 'Core' 
            WHEN FRAME_TYPE='P'   
                THEN 'Price Dependent'
            WHEN FRAME_TYPE = 'D' 
                THEN 'Dependent' 
            ELSE FRAME_TYPE 
        END                     AS PRODUCT_FRAME_TYPE_DESCRIPTION , 
        CASE 
            WHEN NCNR_FLG='Y' 
                THEN TRUE 
            ELSE FALSE
        END::BOOLEAN            AS IS_NCNR , 
        CASE 
            WHEN PRICE_BOOK_FLG='Y' 
                THEN TRUE 
            ELSE FALSE
        END::BOOLEAN            AS IS_PRICE_BOOK ,
        CASE 
            WHEN QUOTE_FLG='Y' 
                THEN TRUE 
            ELSE FALSE::BOOLEAN      
        END                     AS IS_QUOTE ,
        STEP_PRICE_FLG::BOOLEAN AS IS_STEP_PRICING ,
        STAR_LEVEL              AS STAR_LEVEL ,
        ON_TARGET_REMOVAL_DT    AS ON_TARGET_REMOVAL_DATE ,
        ON_TARGET_START_DT      AS ON_TARGET_START_DATE ,
        PHASE_OUT_END_DT        AS PHASE_OUT_END_DATE ,
        PHASE_OUT_START_DT      AS PHASE_OUT_START_DATE ,
        REF_QTY                 AS REFERENCE_QUANTITY ,
        MOQ                     AS MINIMUM_ORDER_QUANTITY ,
        RESALE_PRICE            AS RESALE_PRICE ,
        MDV                     AS MINIMUM_DOLLAR_VALUE ,
        'GPS'                   AS SOURCE_SYSTEM 
    FROM 
    (select * from 
        {{source('STG_GPS2_GPS_ADMIN','STG_PRICE_OUT')}}  
    WHERE GPS_ACTIVE_FLG='Y'     
    QUALIFY(ROW_NUMBER() OVER (PARTITION BY ONPN,EFFECTIVE_DT ORDER BY CREATE_DT desc,QUEUE_ID DESC, PB_EFFECTIVE_DT desc ,BIW_UPD_DTTM DESC) =1)
    )
    

)
,MODELN_PART AS 
(
    SELECT
        OID,
        MPN,
        NCNR
    FROM {{source ('STG_MODELN_PRICING_OWNER','PART')}}
    QUALIFY( row_number() OVER (PARTITION BY OID ORDER BY biw_upd_dttm DESC)=1) 
)

,PRICE_POINT AS (
    SELECT 
        PP.PART_OID,
        MN_PART.MPN as PRODUCT_ID,
        EFFECTIVE_DATE::DATE AS EFFECTIVE_FROM_DATE,
        COALESCE(LEAD(EFFECTIVE_FROM_DATE) OVER(PARTITION BY PART_OID ORDER BY EFFECTIVE_FROM_DATE  ) -1,'9999-12-31') EFFECTIVE_TO_DATE ,
        MAX(CASE 
                WHEN PRICE_POINT_FIELD_OID = 25 
                    THEN PRICE
                ELSE 0 
            END) AS  COGS_PRICE,
        MAX(CASE 
                WHEN PRICE_POINT_FIELD_OID = 24 AND  PRICE_TABLE_DEFINITION_OID =2
                    THEN PRICE
                ELSE 0 
            END) AS  DISTRIBUTOR_BOOK_PRICE_EUR,
        MAX(CASE 
                WHEN PRICE_POINT_FIELD_OID = 24 AND  PRICE_TABLE_DEFINITION_OID =3
                    THEN PRICE
                ELSE 0 
            END) AS  DISTRIBUTOR_BOOK_PRICE_JPY,
        'MODELN' AS SOURCE_SYSTEM 
    FROM {{source('STG_MODELN_PRICING_OWNER','PRICE_POINT')}} PP
    LEFT JOIN MODELN_PART MN_PART
        ON PP.PART_OID = MN_PART.OID
    WHERE 
        (
            PRICE_POINT_FIELD_OID = 25 OR 
            (PRICE_POINT_FIELD_OID = 24 AND  PRICE_TABLE_DEFINITION_OID IN (2,3))
        ) AND 
    {% if var('is_backfill') %}
        BIW_UPD_DTTM >= '{{var('refresh_start_ts')}}'
		AND BIW_UPD_DTTM < '{{V_START_DTTM}}'
    {% else %}
        BIW_UPD_DTTM >= '{{V_LWM}}' 
		AND BIW_UPD_DTTM < '{{V_HWM}}'
    {% endif %}    
   GROUP BY PP.PART_OID,MN_PART.MPN,PP.EFFECTIVE_DATE
)
,PRODUCT_CNTNR_SPEC_DTL_BASE AS
(
    SELECT 
        PART_ID                    AS PRODUCT_ID,
        '1992-01-01'::Date            AS EFFECTIVE_FROM_DATE ,
        COALESCE(LEAD(EFFECTIVE_FROM_DATE) OVER(PARTITION BY PART_ID ORDER BY EFFECTIVE_FROM_DATE  ) -1,'9999-12-31') EFFECTIVE_TO_DATE,
        MD5(COALESCE(PART_ID::STRING,'-1'))::BINARY AS PRODUCT_KEY , 
        FALSE::BOOLEAN AS IS_DELETE ,
        NULL AS DISTRIBUTOR_BOOK_PRICE_USD ,
        NULL AS END_POINT_PRICE_USD ,
        NULL AS FIELD_MINIMUM_QUANTITY ,
        MPQ_QTY AS MINIMUM_PACKAGE_QUANTITY ,
        NULL AS MULTIPLIER ,
        NULL AS REFERENCE_PRICE ,
        0 AS STRATEGIC_MINIMUM_PRICE ,
        0 AS WALK_AWAY_PRICE ,
        '-1' AS FRAME_NAME ,
        NULL AS GPS_PRICE_CATEGORY ,        
        NULL AS GPS_CATEGORY ,
        NULL AS GPS_CATEGORY_DESCRIPTION ,
        NULL AS GPS_MARKET ,
        NULL AS GPS_MARKET_DESCRIPTION ,
        '#'  AS GPS_PROGRAM_CODE ,
        '#'  AS GPS_PROGRAM_DESCRIPTION ,
        '#'  AS GPS_PROGRAM_CODE_DESCRIPTION ,
        NULL AS MARGIN_LEVEL ,
        NULL AS PRODUCT_FRAME_TYPE,
        NULL AS PRODUCT_FRAME_TYPE_DESCRIPTION , 
        TRUE::BOOLEAN AS IS_NCNR , 
        FALSE::BOOLEAN AS IS_PRICE_BOOK ,
        FALSE::BOOLEAN AS IS_QUOTE ,
        FALSE::BOOLEAN AS IS_STEP_PRICING ,
        NULL AS STAR_LEVEL ,
        NULL AS ON_TARGET_REMOVAL_DATE ,
        NULL AS ON_TARGET_START_DATE ,
        NULL AS PHASE_OUT_END_DATE ,
        NULL AS PHASE_OUT_START_DATE ,
        NULL AS REFERENCE_QUANTITY ,
        NULL AS MINIMUM_ORDER_QUANTITY ,
        NULL AS RESALE_PRICE ,
        NULL AS MINIMUM_DOLLAR_VALUE , 
        'MODELN' AS SOURCE_SYSTEM 
    FROM MODELN_PART MN_PART
    INNER JOIN 
        (
            SELECT * 
            FROM 
            {{source('STG_MAINFRAME','PRODUCT_CNTNR_SPEC_DTL_BASE')}} 
            QUALIFY(ROW_NUMBER() OVER (PARTITION BY PART_ID ORDER BY BIW_UPD_DTTM DESC) =1)
        )PRD_CNTNR
            ON PRD_CNTNR.PART_ID = MN_PART.MPN 
     WHERE  
        MN_PART.NCNR = 1
        AND PRD_CNTNR.PART_ID NOT IN ( SELECT PRODUCT_ID FROM GPS_PRICE_OUT)

)


,PRICE_OUT AS
(
    SELECT 
        GPS.* 
    FROM 
        GPS_PRICE_OUT GPS
    UNION 
    SELECT 
        PRD_CNTNR.*
    FROM  
        PRODUCT_CNTNR_SPEC_DTL_BASE PRD_CNTNR
)
,MAX_DATE AS (
    select 
        Max(EFFECTIVE_FROM_DATE) STAGE_FIRST_DATE 
    FROM 
        PRICE_OUT
),
MIN_DATE AS (
    select 
        PRODUCT_ID ,
        MIN(EFFECTIVE_FROM_DATE) AS MIN_EFFECTIVE_FROM_DATE 
    FROM 
        PRICE_OUT
    GROUP BY PRODUCT_ID
)

,PART_DATE_NEEDED AS 
(SELECT 
    PRODUCT_ID,
    EFFECTIVE_FROM_DATE,
    COALESCE(LEAD(EFFECTIVE_FROM_DATE) OVER(PARTITION BY PRODUCT_ID ORDER BY EFFECTIVE_FROM_DATE  ) -1,'9999-12-31') EFFECTIVE_TO_DATE 
 FROM (
        SELECT 
            PRODUCT_ID,
            EFFECTIVE_FROM_DATE
        FROM PRICE_OUT
        UNION 
        SELECT 
            PRODUCT_ID,
            EFFECTIVE_FROM_DATE
        FROM PRICE_POINT        
        )
)


SELECT 
DISTINCT 
    MD5(OBJECT_CONSTRUCT('COL1' ,PDN.PRODUCT_ID ::STRING
                        ,'COL2', PDN.EFFECTIVE_FROM_DATE::STRING
                    )::STRING
    )                   AS GPS_KEY ,
    PDN.PRODUCT_ID ,
    PDN.EFFECTIVE_FROM_DATE ,
    PDN.EFFECTIVE_TO_DATE ,
    Case 
            When PDN.EFFECTIVE_FROM_DATE > GETDATE() 
                Then 'Staged'  
            When PDN.EFFECTIVE_FROM_DATE <= MX.STAGE_FIRST_DATE AND PDN.EFFECTIVE_TO_DATE >= Getdate() 
                Then 'Current'
            When COALESCE(IS_DELETE, FALSE) = TRUE
                THEN 'Deleted'
            Else 'Expired' 
        end                     AS STATUS  ,    
    PRI.IS_DELETE ,
    MD5(PDN.PRODUCT_ID) AS PRODUCT_KEY ,
    PRI.DISTRIBUTOR_BOOK_PRICE_USD ,
    PRI.END_POINT_PRICE_USD ,
    PRI.FIELD_MINIMUM_QUANTITY ,
    PRI.MINIMUM_PACKAGE_QUANTITY ,
    PRI.MULTIPLIER ,
    PRI.REFERENCE_PRICE ,
    PRI.STRATEGIC_MINIMUM_PRICE ,
    PRI.WALK_AWAY_PRICE ,
    PRI.FRAME_NAME ,
    PRI.GPS_PRICE_CATEGORY ,
    PRI.GPS_PROGRAM_DESCRIPTION ,
    PRI.GPS_CATEGORY ,
    PRI.GPS_CATEGORY_DESCRIPTION ,
    PRI.GPS_MARKET ,
    PRI.GPS_MARKET_DESCRIPTION ,
    PRI.GPS_PROGRAM_CODE ,
    PRI.GPS_PROGRAM_CODE_DESCRIPTION ,
    PRI.MARGIN_LEVEL ,
    PRI.PRODUCT_FRAME_TYPE ,
    PRI.PRODUCT_FRAME_TYPE_DESCRIPTION , 
    PRI.IS_NCNR , 
    PRI.IS_PRICE_BOOK ,
    PRI.IS_QUOTE ,
    PRI.IS_STEP_PRICING ,
    PRI.STAR_LEVEL ,
    CASE 
        WHEN PRI.IS_PRICE_BOOK = TRUE AND PRI.EFFECTIVE_FROM_DATE = MI.MIN_EFFECTIVE_FROM_DATE
            THEN TRUE
        ELSE 
            FALSE        
    END::BOOLEAN AS IS_FIRST_TIME_IN_PRICE_BOOK ,
    PRI.ON_TARGET_REMOVAL_DATE ,
    PRI.ON_TARGET_START_DATE ,
    PRI.PHASE_OUT_END_DATE ,
    PRI.PHASE_OUT_START_DATE ,
    PRI.REFERENCE_QUANTITY ,
    PRI.MINIMUM_ORDER_QUANTITY ,
    PRI.RESALE_PRICE ,
    PRI.MINIMUM_DOLLAR_VALUE ,
    MN_PRICE.COGS_PRICE ,
    MN_PRICE.DISTRIBUTOR_BOOK_PRICE_EUR ,
    MN_PRICE.DISTRIBUTOR_BOOK_PRICE_JPY ,
    COALESCE(PRI.SOURCE_SYSTEM,MN_PRICE.SOURCE_SYSTEM) AS SOURCE_SYSTEM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} AS BIW_BATCH_ID ,
    MD5(object_construct ('col1',PRI.PRODUCT_ID::string, 'col2',PDN.EFFECTIVE_FROM_DATE::string, 'col3',PDN.EFFECTIVE_TO_DATE::string, 'col4',STATUS::string, 'col5',IS_DELETE::string
        , 'col6',PRODUCT_KEY::string, 'col7',DISTRIBUTOR_BOOK_PRICE_USD::string, 'col8',END_POINT_PRICE_USD::string, 'col9',FIELD_MINIMUM_QUANTITY::string, 'col10',MINIMUM_PACKAGE_QUANTITY::string
        , 'col11',MULTIPLIER::string, 'col12',REFERENCE_PRICE::string, 'col13',STRATEGIC_MINIMUM_PRICE::string, 'col14',WALK_AWAY_PRICE::string, 'col15',FRAME_NAME::string
        , 'col16',GPS_PRICE_CATEGORY::string, 'col17',GPS_PROGRAM_DESCRIPTION::string, 'col18',COALESCE(PRI.SOURCE_SYSTEM,MN_PRICE.SOURCE_SYSTEM)::string, 'col19',GPS_CATEGORY::string, 'col20',GPS_CATEGORY_DESCRIPTION::string
        , 'col21',GPS_MARKET::string, 'col22',GPS_MARKET_DESCRIPTION::string, 'col23',GPS_PROGRAM_CODE::string, 'col24',GPS_PROGRAM_CODE_DESCRIPTION::string, 'col25',MARGIN_LEVEL::string
        , 'col26',PRODUCT_FRAME_TYPE::string, 'col27',PRODUCT_FRAME_TYPE_DESCRIPTION::string, 'col28',IS_NCNR::string, 'col29',IS_PRICE_BOOK::string, 'col30',IS_QUOTE::string
        , 'col31',IS_STEP_PRICING::string, 'col32',STAR_LEVEL::string, 'col33',IS_FIRST_TIME_IN_PRICE_BOOK::string, 'col34',ON_TARGET_REMOVAL_DATE::string, 'col35',ON_TARGET_START_DATE::string
        , 'col36',PHASE_OUT_END_DATE::string, 'col37',PHASE_OUT_START_DATE::string, 'col38',REFERENCE_QUANTITY::string, 'col39',MINIMUM_ORDER_QUANTITY::string, 'col40',RESALE_PRICE::string
        , 'col41',MINIMUM_DOLLAR_VALUE::string, 'col42',COGS_PRICE::string, 'col43',DISTRIBUTOR_BOOK_PRICE_EUR::string, 'col44',DISTRIBUTOR_BOOK_PRICE_JPY::string)::string ) as BIW_MD5_KEY
FROM  PART_DATE_NEEDED PDN

LEFT JOIN  PRICE_OUT  PRI
    ON PDN.EFFECTIVE_FROM_DATE BETWEEN PRI.EFFECTIVE_FROM_DATE AND PRI.EFFECTIVE_TO_DATE
    AND PDN.PRODUCT_ID = PRI.PRODUCT_ID

LEFT JOIN PRICE_POINT MN_PRICE
    ON PDN.EFFECTIVE_FROM_DATE BETWEEN MN_PRICE.EFFECTIVE_FROM_DATE AND MN_PRICE.EFFECTIVE_TO_DATE
    AND PDN.PRODUCT_ID = MN_PRICE.PRODUCT_ID
LEFT JOIN MAX_DATE MX 
    ON 1=1
LEFT JOIN MIN_DATE MI 
    ON PRI.PRODUCT_ID = MI.PRODUCT_ID