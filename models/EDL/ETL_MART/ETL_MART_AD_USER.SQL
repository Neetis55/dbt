/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_AD_USER

Version     Date            Author              Description
-------     --------        -----------        ----------------------------------
1.0         25 NOV 2022     VINAY SUBRAMANIAN   Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['AD_USER_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_AD_USER'-%}
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
         description = 'Building table AD_USER for MART'
        ,transient=true
        ,materialized='view'
        ,schema ='ETL_MART'
        ,alias='AD_USER'
		,unique_key= v_pk_list
        ,tags =['MART']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH AD_USER AS
(
SELECT
    MD5(ONUID::STRING) AS AD_USER_KEY,
    ONUID AS USERID,
    AD_GROUP,
    DN,
    OBJECTCLASS,
    ONCOMPANYDN,
    MAIL AS EMAILADDRESS,
    ONMODIFIER,
    ONACCOUNTTYPE,
    ONSPONSORDN,
    ONPREFERREDNAME,
    SN,
    ONMODIFYTIMESTAMP,
    TELEPHONENUMBER,
    ONDEPARTMENTDN,
    ONDISPLAY,
    OTHERSYSTEMS,
    ONMAILDROPDN,
    CN AS DISPLAYNAME,
    ONCOMPUTERDN,
    ONSHOWPICTURE,
    GIVENNAME,
    ONLOCATIONDN,
    LOGINDISABLED,
    ONSYNCDATE,
    TITLE,
    FACSIMILETELEPHONENUMBER,
    ROOMNUMBER,
    ONSYNCTYPE,
    ONSUPERVISORDN,
    ONPURCHASEORDERDN,
    C,
    ONCID,
    ONPAGERDN,
    INITIALS,
    MOBILE,
    ONLICENSEPLATENUMBER,
    BASE,
    HASREPORTS,
    ADVS_ONDEPARTMENTDN AS DEPARTMENT,
    ADVS_ONCOMPANYDN,
    ADVS_ONSUPERVISORDN AS MANAGER,
    ADVS_ONSPONSORDN,
    ADVS_ONLOCATIONDN,
    ADVS_ONMAILDROPDN,
    ONPERSONALPHONE,
    ONPERSONALPAGER,
    ONSSTANUMBER,
    ONBADGEEXPIREDATE,
    ONBADGEISSUEDATE,
    ONBIRTHDATESTR,
    DESCRIPTION,
    ONEXTERNALMANAGERNAME,
    ONEXTERNALMANAGERPHONE,
    ONSSN,
    MIRROR_SYNC_TIMESTAMP,
    ONPHONEDN,
    ONORACLEPERSONID,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID
FROM 
{{source('STG_ONDEX_GONDEX_USER','STG1_ONDEX_MDW_STAGE')}} 
QUALIFY( ROW_NUMBER() OVER (PARTITION BY ONUID ORDER BY BIW_UPD_DTTM DESC)=1)
)

SELECT 
    *,
    md5(object_construct ('col1',AD_GROUP::string, 'col2',DN::string, 'col3',OBJECTCLASS::string,
    'col4',ONCOMPANYDN::string, 'col5',EMAILADDRESS::string, 'col6',ONMODIFIER::string, 'col7',ONACCOUNTTYPE::string,
    'col8',ONSPONSORDN::string, 'col9',ONPREFERREDNAME::string, 'col10',SN::string, 'col11',ONMODIFYTIMESTAMP::string,
    'col12',TELEPHONENUMBER::string, 'col13',ONDEPARTMENTDN::string, 'col14',ONDISPLAY::string, 'col15',OTHERSYSTEMS::string,
    'col16',ONMAILDROPDN::string, 'col17',DISPLAYNAME::string, 'col18',ONCOMPUTERDN::string, 'col19',ONSHOWPICTURE::string,
    'col20',GIVENNAME::string, 'col21',ONLOCATIONDN::string, 'col22',LOGINDISABLED::string, 'col23',ONSYNCDATE::string,
    'col24',TITLE::string, 'col25',FACSIMILETELEPHONENUMBER::string, 'col26',ROOMNUMBER::string, 'col27',ONSYNCTYPE::string,
    'col28',ONSUPERVISORDN::string, 'col29',ONPURCHASEORDERDN::string, 'col30',C::string, 'col31',ONCID::string,
    'col32',ONPAGERDN::string, 'col33',INITIALS::string, 'col34',MOBILE::string, 'col35',ONLICENSEPLATENUMBER::string,
    'col36',BASE::string, 'col37',HASREPORTS::string, 'col38',DEPARTMENT::string, 'col39',ADVS_ONCOMPANYDN::string,
    'col40',MANAGER::string, 'col41',ADVS_ONSPONSORDN::string, 'col42',ADVS_ONLOCATIONDN::string,
    'col43',ADVS_ONMAILDROPDN::string, 'col44',ONPERSONALPHONE::string, 'col45',ONPERSONALPAGER::string, 'col46',ONSSTANUMBER::string,
    'col47',ONBADGEEXPIREDATE::string, 'col48',ONBADGEISSUEDATE::string, 'col49',ONBIRTHDATESTR::string, 'col50',DESCRIPTION::string,
    'col51',ONEXTERNALMANAGERNAME::string, 'col52',ONEXTERNALMANAGERPHONE::string, 'col53',ONSSN::string,
    'col54',MIRROR_SYNC_TIMESTAMP::string, 'col55',ONPHONEDN::string, 'col56',ONORACLEPERSONID::string, 'col57',BIW_INS_DTTM::string,
    'col58',BIW_UPD_DTTM::string, 'col59',BIW_BATCH_ID::string)::string ) as BIW_MD5_KEY
FROM AD_USER