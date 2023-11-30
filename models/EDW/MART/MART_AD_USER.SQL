/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select MART_AD_USER

Version     Date            Author              Description
-------     --------        -----------        ----------------------------------
1.0         18 NOV 2022     SRUTHI KASBE       Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['USERID']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list(  ref('ETL_MART_AD_USER')  ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_AD_USER'-%}
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
        ,materialized='table'
        ,schema ='MART'
        ,alias='AD_USER'
		,unique_key= v_pk_list
        ,tags =['MART']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}


SELECT
    AD_USER_KEY,
    USERID,
    AD_GROUP,
    DN,
    OBJECTCLASS,
    ONCOMPANYDN,
    EMAILADDRESS,
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
    DISPLAYNAME,
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
    DEPARTMENT,
    ADVS_ONCOMPANYDN,
    MANAGER,
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
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    BIW_MD5_KEY
FROM
	{{ref('ETL_MART_AD_USER')}}
