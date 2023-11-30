/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ODS_NPD_MSPROJECTS_ENGAGEMENTS_WEEKLY_SNAPSHOT
-- dbt build --full-refresh --select ODS_NPD_MSPROJECTS_ENGAGEMENTS_WEEKLY_SNAPSHOT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         25-JAN-2023     KALI DANDAPANI      Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['ENGAGEMENTS_KEY']-%}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ODS_NPD_MSPROJECTS_ENGAGEMENTS_WEEKLY_SNAPSHOT'-%}
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
         description = 'Building ODS table ENGAGEMENTS_WEEKLY_SNAPSHOT for NPD LANDING PROJECT'
        ,transient=false   
         ,materialized='incremental'
        ,schema ='ODS_NPD'
        ,alias= 'MSPROJECTS_ENGAGEMENTS_WEEKLY_SNAPSHOT'
        ,tags =['ODS_NPD']
        ,unique_key= v_pk_list
        ,merge_update_columns = ['ENGAGEMENTID','COMMITTEDFINISHDATE','COMMITTEDMAXUNITS','COMMITTEDSTARTDATE','COMMITTEDWORK','ENGAGEMENTCREATEDDATE','ENGAGEMENTMODIFIEDDATE','ENGAGEMENTNAME','ENGAGEMENTREVIEWEDDATE','ENGAGEMENTSTATUS','ENGAGEMENTSUBMITTEDDATE','MODIFIEDBYRESOURCEID','MODIFIEDBYRESOURCENAME','PROJECTID','PROJECTNAME','PROPOSEDFINISHDATE','PROPOSEDMAXUNITS','PROPOSEDSTARTDATE','PROPOSEDWORK','RESOURCEID','RESOURCENAME','REVIEWEDBYRESOURCEID','REVIEWEDBYRESOURCENAME','SUBMITTEDBYRESOURCEID','SUBMITTEDBYRESOURCENAME','LINKEDCOMMENT','LINKEDTIMEPHASEDINFO','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT 
STG.ENGAGEMENTS_KEY
,STG.SNAPSHOT_WEEK_KEY
,STG.ENGAGEMENTID
,STG.COMMITTEDFINISHDATE
,STG.COMMITTEDMAXUNITS
,STG.COMMITTEDSTARTDATE
,STG.COMMITTEDWORK
,STG.ENGAGEMENTCREATEDDATE
,STG.ENGAGEMENTMODIFIEDDATE
,STG.ENGAGEMENTNAME
,STG.ENGAGEMENTREVIEWEDDATE
,STG.ENGAGEMENTSTATUS
,STG.ENGAGEMENTSUBMITTEDDATE
,STG.MODIFIEDBYRESOURCEID
,STG.MODIFIEDBYRESOURCENAME
,STG.PROJECTID
,STG.PROJECTNAME
,STG.PROPOSEDFINISHDATE
,STG.PROPOSEDMAXUNITS
,STG.PROPOSEDSTARTDATE
,STG.PROPOSEDWORK
,STG.RESOURCEID
,STG.RESOURCENAME
,STG.REVIEWEDBYRESOURCEID
,STG.REVIEWEDBYRESOURCENAME
,STG.SUBMITTEDBYRESOURCEID
,STG.SUBMITTEDBYRESOURCENAME
,STG.LINKEDCOMMENT
,STG.LINKEDTIMEPHASEDINFO
,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
   ,{{V_BIW_BATCH_ID}}::NUMBER as BIW_BATCH_ID ,STG.BIW_MD5_KEY
    FROM 
        {{ref ('ETL_ODS_NPD_MSPROJECTS_ENGAGEMENTS_WEEKLY_SNAPSHOT')}}  STG  
{% if is_incremental() %}
  LEFT JOIN {{ this }} TGT
  on STG.ENGAGEMENTS_KEY= TGT.ENGAGEMENTS_KEY
  WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
{% endif %}
