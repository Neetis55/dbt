
/*---------------------------------------------------------------------------
Command to run model:
--dbt build --select ETL_MART_ENGINEERING_PART_APS
-- dbt run --full-refresh --select ETL_MART_ENGINEERING_PART_APS

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         20/09/2022      Ramya      Initial Version
2.0         28/OCT/2022     KALI       Added missing APS parts using Oracle Data warehouse logic 
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['PART_ID']-%}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_ENGINEERING_PART_APS'-%}
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
         description = 'Building ETL Part table with APS Attributes'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_ENGINEERING'
        ,alias='PART_APS'
        ,tags =['MART_ENGINEERING','MART_SALES']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}


WITH  ON_BOM AS 
(
    SELECT  
        PROD_ITEM_NAME,
        CONSUM_ITEM_NAME,
        PROD_FCLTY_NAME,
        CONSUM_FCLTY_NAME,
        CASE 
            WHEN RES_GRP_NAME IS NOT NULL 
                THEN 'BOM'
            ELSE 'BOD' 
        END AS RTNG_TYPE_CD ,
        BOM_NAME,
        CASE 
            WHEN PROD_FCLTY_NAME= 'SG1' 
                THEN 'SW' 
            WHEN  PROD_FCLTY_NAME LIKE '%^_%' ESCAPE '^'
                THEN SUBSTRING(PROD_FCLTY_NAME,1, POSITION('_', PROD_FCLTY_NAME, 1) - 1)
            ELSE PROD_FCLTY_NAME
        END AS APS_SITE_ID,-- KINAXIS_SITE
        CASE 
            WHEN CONSUM_FCLTY_NAME= 'SG1' 
                THEN 'SW' 
            WHEN  POSITION('_', CONSUM_FCLTY_NAME, 1)>0
                THEN SUBSTRING(CONSUM_FCLTY_NAME,1, POSITION('_', CONSUM_FCLTY_NAME, 1) - 1)
            ELSE CONSUM_FCLTY_NAME
        END AS BOM_COMPNT_APS_SITE_ID,
        CASE 
            WHEN PROD_FCLTY_NAME= 'SDB'
                THEN 'WDC'
            WHEN PROD_FCLTY_NAME LIKE '%^_%' ESCAPE '^'  AND NOT(PROD_FCLTY_NAME LIKE 'ADC^_%^_%' ESCAPE '^' )
                THEN SUBSTRING(PROD_FCLTY_NAME, POSITION('_', PROD_FCLTY_NAME, 1) + 1)
            WHEN PROD_FCLTY_NAME LIKE 'ADC^_%^_%' ESCAPE '^'  OR PROD_FCLTY_NAME IN ('ADC','IRC','SRC','GDC')
                THEN 'SGC'
        END  AS APS_STAGE_CD,--KINAXIS_SITE_STAGE
        CASE 
            WHEN CONSUM_FCLTY_NAME= 'SDB'
                THEN 'WDC'
            WHEN CONSUM_FCLTY_NAME LIKE '%^_%' ESCAPE '^'  AND NOT(CONSUM_FCLTY_NAME LIKE 'ADC^_%^_%' ESCAPE '^' )
                THEN SUBSTRING(CONSUM_FCLTY_NAME, POSITION('_', CONSUM_FCLTY_NAME, 1) + 1)
            WHEN CONSUM_FCLTY_NAME LIKE 'ADC^_%^_%' ESCAPE '^'  OR CONSUM_FCLTY_NAME IN ('ADC','IRC','SRC','GDC')
                THEN 'SGC'
        END  AS  BOM_COMPNT_APS_STAGE_CD
    FROM 
    {{source('STG_PLAN_APS_OWNER','ON_BOMROUTING')}}
    WHERE BOM_NAME IS NOT NULL 
    AND BOM_COMPNT_APS_SITE_ID IS NOT NULL 
    -- EXCLUDE BOMS WITH NO COMPONENTS.
    AND NOT (RTNG_TYPE_CD = 'BOM' AND CONSUM_ITEM_NAME IS NULL) 
)

,SUPPLY_PATH_PART_MATCH AS 
(   SELECT 
        DISTINCT 
        CONSUM_ITEM_NAME AS PART_ID,
        BOM_COMPNT_APS_STAGE_CD,
        APS_STAGE_CD
    FROM ON_BOM
    WHERE RTNG_TYPE_CD = 'BOM'
    AND PROD_ITEM_NAME = CONSUM_ITEM_NAME
    AND APS_SITE_ID = BOM_COMPNT_APS_SITE_ID
    AND APS_STAGE_CD <> BOM_COMPNT_APS_STAGE_CD
 )

 , SUPPLY_PATH_APS AS 
 (
     SELECT 
        APS.PROD_ITEM_NAME ,
        APS.CONSUM_ITEM_NAME,
        APS.RTNG_TYPE_CD,
        APS.APS_STAGE_CD,
        APS.BOM_COMPNT_APS_STAGE_CD,
        APS.APS_SITE_ID,
        APS.BOM_COMPNT_APS_SITE_ID        
     FROM
     ON_BOM AS APS
     LEFT JOIN SUPPLY_PATH_PART_MATCH PRDCT
             ON APS.PROD_ITEM_NAME = PRDCT.PART_ID
             AND APS.APS_STAGE_CD= PRDCT.BOM_COMPNT_APS_STAGE_CD
    LEFT JOIN SUPPLY_PATH_PART_MATCH CONS_PRDCT
            ON APS.CONSUM_ITEM_NAME = CONS_PRDCT.PART_ID
            AND APS.BOM_COMPNT_APS_STAGE_CD= CONS_PRDCT.BOM_COMPNT_APS_STAGE_CD
     WHERE (
            PRDCT.PART_ID IS NOT NULL 
            OR 
            (CONS_PRDCT.PART_ID IS NOT NULL AND APS. RTNG_TYPE_CD = 'BOD')
            )
     --AND  PROD_ITEM_NAME ='WBTU1G99BB89' --OR  CONSUM_ITEM_NAME ILIKE '%WBTU1G99BB89%'
 )
 -- Recursive tracking of part to component path 
, SUPPLY_PATH_APS_REC 
 (
     PART_ID,
     RTNG_TYPE_CD,
     APS_STAGE_CD,
     APS_SITE_ID,
     LAST_APS_STAGE_CD,
     STEP_NUMBER     
 )AS 
 (
    SELECT 
        APS.PROD_ITEM_NAME  AS PART_ID,
        RTNG_TYPE_CD,
        APS.APS_STAGE_CD,
        MAX(APS_SITE_ID) AS APS_SITE_ID,
        APS_STAGE_CD AS LAST_APS_STAGE_CD,
        1 STEP_NUMBER
    FROM 
        SUPPLY_PATH_APS APS
    WHERE 
        APS.RTNG_TYPE_CD = 'BOM'
        AND APS.PROD_ITEM_NAME <> CONSUM_ITEM_NAME
    GROUP BY 
        APS.PROD_ITEM_NAME
        ,APS.APS_STAGE_CD
        ,RTNG_TYPE_CD
    UNION ALL
    SELECT 
        PARENT.PART_ID,
        CHILD.RTNG_TYPE_CD,
        CHILD.APS_STAGE_CD,
        CHILD.APS_SITE_ID,
        CASE 
            WHEN CHILD.RTNG_TYPE_CD = 'BOM'
                THEN CHILD.APS_STAGE_CD
            ELSE PARENT.LAST_APS_STAGE_CD
        END AS LAST_APS_STAGE_CD,
        PARENT.STEP_NUMBER+1  STEP_NUMBER  
    FROM 
        SUPPLY_PATH_APS_REC PARENT 
    INNER JOIN 
        SUPPLY_PATH_APS CHILD
        ON  PARENT.PART_ID = CHILD.CONSUM_ITEM_NAME 
        AND PARENT.APS_SITE_ID = CHILD.BOM_COMPNT_APS_SITE_ID  
        AND PARENT.APS_STAGE_CD= CHILD.BOM_COMPNT_APS_STAGE_CD  
    WHERE 
        CHILD.PROD_ITEM_NAME = CHILD.CONSUM_ITEM_NAME
 )

,PART_ATTRS AS
(
SELECT 
    PART_ID,
    AREA,
    SUB_AREA,
    ASSY_RATIO,
    AVERAGE_USAGE
FROM {{source ('STG_PLAN_APS_OWNER','PART_ATTRS')}}
QUALIFY( row_number() OVER (PARTITION BY PART_ID ORDER BY BIW_UPD_DTTM DESC)=1)
)

,APS_MASTER_PART_LIST AS
(SELECT 
    PART_ID,
    BASE_PART_ID,
    DEFLT_APS_STAGE_CD
FROM 
    (-- 1. Produced Beginning of BOM
    SELECT 
        BR.PROD_ITEM_NAME AS PART_ID,
        MAX(SUBSTR(BR.PROD_ITEM_NAME, POSITION('_',BR.PROD_ITEM_NAME) + 1)) AS BASE_PART_ID,
        'BOB' AS DEFLT_APS_STAGE_CD,
        1 AS QUERY_RANK
    FROM 
        {{source('STG_PLAN_APS_OWNER','ON_BOMROUTING')}} BR
    WHERE 
        BR.PROD_ITEM_NAME NOT LIKE 'COE%'
    GROUP BY 
        BR.PROD_ITEM_NAME 
    UNION ALL
    -- 2. Consumed BOB (Beginning of BOM)
    SELECT 
        BR.CONSUM_ITEM_NAME AS PART_ID,
        MAX(SUBSTR(BR.CONSUM_ITEM_NAME, POSITION('_',BR.CONSUM_ITEM_NAME) + 1)) AS BASE_PART_ID,
        'BOB' AS DEFLT_APS_STAGE_CD,
        2 AS QUERY_RANK
    FROM 
        {{source('STG_PLAN_APS_OWNER','ON_BOMROUTING')}} BR
    WHERE 
        BR.CONSUM_ITEM_NAME NOT LIKE 'COE%'
    GROUP BY 
        BR.CONSUM_ITEM_NAME
    UNION ALL
    -- 3. APS multistage parts
    SELECT  
        PART_ID||'-'|| LAST_APS_STAGE_CD AS PART_ID, 
        MAX(PART_ID) AS BASE_PART_ID,
        MAX(APS_STAGE_CD) AS DEFLT_APS_STAGE_CD,
        3 AS QUERY_RANK
    FROM 
        SUPPLY_PATH_APS_REC
    GROUP BY 
        PART_ID||'-'|| LAST_APS_STAGE_CD 
    UNION ALL
    SELECT 
        PART_ID,
        PART_ID AS BASE_PART_ID,
        NULL AS DEFLT_APS_STAGE_CD,
        4 AS QUERY_RANK
    FROM 
        PART_ATTRS  
    GROUP BY 
        PART_ID  
    )
QUALIFY (ROW_NUMBER () OVER (PARTITION BY PART_ID ORDER BY QUERY_RANK )= 1)      
)

,PART_FE_ATTRS AS 
(
SELECT 
    FINISHED_GOOD,
    CAPC_GROUP_CD,
    WFA_SITES,
    WPR_SITES
FROM  {{source ('STG_PLAN_APS_OWNER','PART_FE_ATTRS')}} 
QUALIFY( row_number() OVER (PARTITION BY FINISHED_GOOD ORDER BY BIW_UPD_DTTM DESC)=1) 
)

,APSTORR_PART_ATTR AS
(
SELECT 
    ITEM,
    CAPC_CONSTR_COLOR
FROM {{source ('STG_PLAN_APS_OWNER','APSTORR_PART_ATTR')}}
QUALIFY( row_number() OVER (PARTITION BY ITEM ORDER BY BIW_UPD_DTTM DESC)=1)  
)

,CWB_PACKAGE_GROUP_XREF AS
(
SELECT 
    PART_ID,
    PACKAGE_GROUP_CD
FROM {{source ('STG_PLAN_APS_OWNER','CWB_PACKAGE_GROUP_XREF')}} 
QUALIFY( row_number() OVER (PARTITION BY PART_ID  ORDER BY BIW_UPD_DTTM DESC)=1) 
)

,LT_HISTORY_DETAIL_VW AS 
(
SELECT
    PART_ID,
    DCE_CT,
    FAMILY_LT,
    POINT_LT,
    TREND_LT,
    OVERRIDE_LT,
    TBOT_LT
FROM (
    SELECT  
        PART_ID,
        DCE_CT,
        FAMILY_LT,
        POINT_LT,
        TREND_LT,
        OVERRIDE_LT,
        TBOT_LT,
        BUCKET_SEQ
FROM 
    {{source ('STG_PLAN_APS_OWNER','LT_HISTORY_DETAIL_ALL')}} 
QUALIFY( row_number() OVER (PARTITION BY PART_ID ORDER BY UPDATE_DT DESC)=1))
where BUCKET_SEQ=0
)

,PART_SITES AS
(
SELECT 
    PART_ID,
    MBOI_ASSY_SITES,
    MBOI_TEST_SITES,
    APS_ASSY_SITES,
    APS_TEST_SITES,
    AGILE_ASSY_SITES,
    AGILE_TEST_SITES                          
FROM {{source ('STG_PLAN_APS_OWNER','PART_SITES')}} 
QUALIFY( row_number() OVER (PARTITION BY PART_ID ORDER BY BIW_UPD_DTTM DESC)=1) 
)

,STG_CCD_CONSTR_COLOR_CURR AS
(
SELECT
    PART_ID, 
    COLOR_ADJUSTED
FROM {{source ('STG_PLAN_APS_OWNER','CCD_PART_CONSTR_COLOR_CURR')}}
QUALIFY( row_number() OVER (PARTITION BY PART_ID ORDER BY BIW_UPD_DTTM DESC)=1)
)

,STG_CCD_CONSTR_COLOR_OVERRIDE AS
(
SELECT 
    PART_ID,
    COLOR_OVERRIDE,
    CREATED_USERID,
    REASON_DESC
FROM {{source ('STG_PLAN_APS_OWNER','CCD_PART_CONSTR_COLOR_OVERRIDE')}}
QUALIFY( row_number() OVER (PARTITION BY PART_ID ORDER BY BIW_UPD_DTTM DESC)=1)
)

,ITEMMASTER AS 
(
SELECT
    ITEM,
    DECODE(ITEM,NULL,'N','Y') AS APS_REL_FLG
FROM {{source ('STG_PLAN_APS_OWNER','ITEMMASTER')}}
QUALIFY( row_number() OVER (PARTITION BY ITEM ORDER BY BIW_UPD_DTTM DESC)=1) 
)

,ODSTOOM_PLAN_CNCT_DIR AS
(
SELECT
    OPN,
    SCA_UID,
    SCA_USERNAME,
    BUPM_USERNAME,
    BUPM_UID
FROM {{source ('STG_PLAN_APSWEB_OWNER','ODSTOOM_PLAN_CONTACT_DIR')}}
QUALIFY( row_number() OVER (PARTITION BY OPN ORDER BY BIW_UPD_DTTM DESC)=1) 
)

,SD_USERID AS
(
    SELECT 
        DN,
        ON_UID,
        MAIL,
        ADVS_ON_SUPRVISR_DN,
        GIVEN_NAME,
        SURNAME,
        EFFECTIVE_TO_DATE
    FROM {{source ('STG_ONE_STAGE','SD_USERID')}} 
    WHERE EFFECTIVE_TO_DATE = '31-DEC-3999'
    QUALIFY( row_number() OVER (PARTITION BY DN ORDER BY BIW_UPD_DTTM DESC)=1)  
)

,PLANNING_DIR AS
(
    SELECT 
        X.ON_UID,
        X.ADVS_ON_SUPRVISR_DN AS DIR_UID,
        (Y.GIVEN_NAME || ' ' || Y.SURNAME) AS DIR_NAME
    FROM SD_USERID X, SD_USERID Y
    WHERE     X.ADVS_ON_SUPRVISR_DN = Y.ON_UID
)

,ODSTOOM_PLAN_CONTACT_DIR AS
(
    SELECT 
        CNTC_DIR.OPN,
        CNTC_DIR.SCA_UID,
        CNTC_DIR.SCA_USERNAME,
        CNTC_DIR.BUPM_USERNAME,
        CNTC_DIR.BUPM_UID,
        USERID.MAIL,
        PLAN_DIR.DIR_UID,
        PLAN_DIR.DIR_NAME
    FROM ODSTOOM_PLAN_CNCT_DIR CNTC_DIR
    LEFT OUTER JOIN SD_USERID USERID
       ON CNTC_DIR.SCA_UID = UPPER(USERID.ON_UID)
    LEFT OUTER JOIN PLANNING_DIR PLAN_DIR
        ON CNTC_DIR.BUPM_UID = UPPER(PLAN_DIR.ON_UID)
)

,ITEM_ATTR_O AS (
SELECT
    ITEM,
    BUILD_CATEGORY AS APS_BUILD_CATEGORY_CODE,
    PART_CLASS AS IO_PART_CLASS, 
    IO_CATEGORY ,
    OVERRIDE_CATEGORY AS IO_OVERRIDE_CATEGORY
FROM {{source ('STG_PLAN_APSWEB_OWNER','ITEM_ATTR_O')}}
QUALIFY( row_number() OVER (PARTITION BY ITEM ORDER BY BIW_UPD_DTTM DESC)=1)      
)

,MLQ_CURR_TBL AS 
(
SELECT 
    PART_ID,
    MLQ 
FROM {{source ('STG_PLAN_DM_RFORECASTING','MLQ_CURR_TBL')}}
QUALIFY( row_number() OVER (PARTITION BY PART_ID ORDER BY BIW_UPD_DTTM DESC)=1)

)

,STG_PLAN_DM_RFORECASTING AS
(
    SELECT 
        PART_ID,
        PRIMARY_CONSTR_RESGROUP
    FROM {{source ('STG_PLAN_DM_RFORECASTING','CCD_PART_CONSTR_COLOR_HIST')}}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PART_ID ORDER BY BIW_UPD_DTTM DESC) =1)
)

SELECT 
    PART_LIST.PART_ID,
    PART_LIST.DEFLT_APS_STAGE_CD,
    PART_LIST.BASE_PART_ID,
    APS_PART.AREA,
    APS_PART.SUB_AREA,
    APS_PART.ASSY_RATIO,
    APS_PART.AVERAGE_USAGE,

    FE_ATTR.CAPC_GROUP_CD,
    FE_ATTR.WFA_SITES,
    FE_ATTR.WPR_SITES,

    APSTORR_ATTR.CAPC_CONSTR_COLOR,

    CWB.PACKAGE_GROUP_CD,

    LT_HIST.DCE_CT,
    LT_HIST.FAMILY_LT,
    LT_HIST.POINT_LT,
    LT_HIST.TREND_LT,
    LT_HIST.OVERRIDE_LT,
    LT_HIST.TBOT_LT,
    
    SITE.MBOI_ASSY_SITES,
    SITE.MBOI_TEST_SITES,
    SITE.APS_ASSY_SITES,
    SITE.APS_TEST_SITES,
    SITE.AGILE_ASSY_SITES,
    SITE.AGILE_TEST_SITES,

    CCD_PART.COLOR_ADJUSTED,

    CONSTR_COLOR.COLOR_OVERRIDE,
    CONSTR_COLOR.CREATED_USERID,
    CONSTR_COLOR.REASON_DESC,
    
    IT_MSTR.APS_REL_FLG,
    
    ODSTOOM.OPN,
    ODSTOOM.SCA_UID,
    ODSTOOM.SCA_USERNAME,
    ODSTOOM.BUPM_USERNAME,
    ODSTOOM.BUPM_UID,
    ODSTOOM.MAIL,
    ODSTOOM.DIR_UID,
    ODSTOOM.DIR_NAME,
    COALESCE(ITM.APS_BUILD_CATEGORY_CODE,' ') AS APS_BUILD_CATEGORY_CODE ,
    ITM.IO_PART_CLASS, 
    ITM.IO_CATEGORY ,
    ITM.IO_OVERRIDE_CATEGORY,

    MLQ_CURR.MLQ,
    NVL(RFORECASTING.PRIMARY_CONSTR_RESGROUP,'') AS CAPC_CONSTR_PRIM_CONSTR_RG,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID
FROM  APS_MASTER_PART_LIST PART_LIST
LEFT JOIN PART_ATTRS AS APS_PART
    ON PART_LIST.PART_ID = APS_PART.PART_ID 
LEFT JOIN PART_FE_ATTRS AS FE_ATTR 
    ON APS_PART.PART_ID=FE_ATTR.FINISHED_GOOD

LEFT JOIN APSTORR_PART_ATTR AS APSTORR_ATTR
    ON APS_PART.PART_ID=APSTORR_ATTR.ITEM 

LEFT JOIN CWB_PACKAGE_GROUP_XREF AS CWB
    ON APS_PART.PART_ID=CWB.PART_ID 

LEFT JOIN LT_HISTORY_DETAIL_VW AS LT_HIST
    ON APS_PART.PART_ID=LT_HIST.PART_ID

LEFT JOIN PART_SITES AS SITE  
    ON APS_PART.PART_ID=SITE.PART_ID

LEFT JOIN  STG_CCD_CONSTR_COLOR_CURR AS CCD_PART
    ON APS_PART.PART_ID=CCD_PART.PART_ID 

LEFT JOIN  STG_CCD_CONSTR_COLOR_OVERRIDE AS  CONSTR_COLOR
    ON APS_PART.PART_ID=CONSTR_COLOR.PART_ID 

LEFT JOIN  ITEMMASTER AS IT_MSTR
ON APS_PART.PART_ID=IT_MSTR.ITEM

LEFT JOIN   ODSTOOM_PLAN_CONTACT_DIR AS ODSTOOM
    ON APS_PART.PART_ID=ODSTOOM.OPN 

LEFT JOIN  ITEM_ATTR_O AS ITM
    ON APS_PART.PART_ID=ITM.ITEM 

LEFT JOIN  MLQ_CURR_TBL AS MLQ_CURR
    ON APS_PART.PART_ID=MLQ_CURR.PART_ID

LEFT OUTER JOIN STG_PLAN_DM_RFORECASTING RFORECASTING 
    ON APS_PART.PART_ID=RFORECASTING.PART_ID