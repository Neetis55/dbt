/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select MART_SALES_OM_EP_LINEDOWN_INFO
Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         23 NOV 2022     ALFRED LOO          Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['LINEDOWN_INFO_RULE_KEY']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_OM_EP_LINEDOWN_INFO') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_OM_EP_LINEDOWN_INFO') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_SALES_OM_EP_LINEDOWN_INFO'),'stg.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'MART_SALES_OM_EP_LINEDOWN_INFO'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}


{{
    config(
         description = 'Building table OM_EP_LINEDOWN_INFO for sales mart '
        ,transient=false
        ,materialized='table'
        ,schema ='MART_SALES'
        ,alias='MART_SALES_OM_EP_LINEDOWN_INFO'
        ,unique_key= v_pk_list
        ,merge_update_columns = [
            'LDStatus', 'LDColor', 'MPN', 'CPN', 'ShipTo', 'ConsumptionCode', 'EndCustomer', 'CustomerName', 'CorporationName', 'CreatedDate',
            'OpenDays', 'DropDeadStartDate', 'DropDeadWeeklyRunRate', 'DropDeadQuantity', 'ContractualLiability', 'CurrentLeadtime', 'RootcauseForLDEscalation',
            'ConstrainedDevice', 'ContractCompliance', 'RecoveryPeriod', 'CSR', 'Attribute1', 'Attribute2', 'Attribute3', 'Attribute4', 'Attribute5',
            'Attribute6', 'Attribute7', 'Attribute8', 'Attribute9', 'Attribute10', 'BIW_INS_DTTM', 'BIW_UPD_DTTM', 'BIW_BATCH_ID' ] 
        ,tags ='MART_OUTSYS_SALES'
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH 

OM_EP_LINEDOWN_ORDER_INFO AS (

    SELECT
        LINEDOWN_INFO_RULE_KEY,
        LindownId,
        LDStatus,
        LDColor,
        MPN,
        CPN,
        ShipTo,
        ConsumptionCode,
        EndCustomer,
        CustomerName,
        CorporationName,
        CreatedDate,
        OpenDays,
        DropDeadStartDate,
        DropDeadWeeklyRunRate,
        DropDeadQuantity,
        ContractualLiability,
        CurrentLeadtime,
        RootcauseForLDEscalation,
        ConstrainedDevice,
        ContractCompliance,
        RecoveryPeriod,
        CSR,
        Attribute1,
        Attribute2,
        Attribute3,
        Attribute4,
        Attribute5,
        Attribute6,
        Attribute7,
        Attribute8,
        Attribute9,
        Attribute10,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ as BIW_INS_DTTM ,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ as BIW_UPD_DTTM ,
        '{{V_BIW_BATCH_ID}}' as BIW_BATCH_ID,
        BIW_MD5_KEY 
    FROM {{ref('ETL_MART_SALES_OM_EP_LINEDOWN_INFO')}}
    
)
 
SELECT 
        SRC.LINEDOWN_INFO_RULE_KEY,
        SRC.LindownId,
        SRC.LDStatus,
        SRC.LDColor,
        SRC.MPN,
        SRC.CPN,
        SRC.ShipTo,
        SRC.ConsumptionCode,
        SRC.EndCustomer,
        SRC.CustomerName,
        SRC.CorporationName,
        SRC.CreatedDate,
        SRC.OpenDays,
        SRC.DropDeadStartDate,
        SRC.DropDeadWeeklyRunRate,
        SRC.DropDeadQuantity,
        SRC.ContractualLiability,
        SRC.CurrentLeadtime,
        SRC.RootcauseForLDEscalation,
        SRC.ConstrainedDevice,
        SRC.ContractCompliance,
        SRC.RecoveryPeriod,
        SRC.CSR,
        SRC.Attribute1,
        SRC.Attribute2,
        SRC.Attribute3,
        SRC.Attribute4,
        SRC.Attribute5,
        SRC.Attribute6,
        SRC.Attribute7,
        SRC.Attribute8,
        SRC.Attribute9,
        SRC.Attribute10,
        SRC.BIW_INS_DTTM ,
        SRC.BIW_UPD_DTTM ,
        SRC.BIW_BATCH_ID,
        SRC.BIW_MD5_KEY 
FROM OM_EP_LINEDOWN_ORDER_INFO SRC
/*{% if is_incremental() %}
  LEFT JOIN {{ this }} TGT
  on SRC.LINEDOWN_INFO_RULE_KEY= TGT.LINEDOWN_INFO_RULE_KEY
WHERE   TGT.BIW_MD5_KEY <> SRC.BIW_MD5_KEY OR 
        TGT.BIW_MD5_KEY IS NULL
{% endif %}*/
