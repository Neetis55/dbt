/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select DBT_MART_OUTSYS_OM_EP_LINEDOWN_INFO 

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         11/13/2022      Alfred        Initial Version 
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['LINEDOWN_INFO_RULE_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}
 
{################# Batch control insert and update SQL #################}
-- Step 1 Batch process info  
{%- set v_dbt_job_name = 'ETL_MART_SALES_OM_EP_LINEDOWN_INFO'-%}
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{################# Snowflake Object Configuration #################}
{{
    config(
         description = 'Building ETL MART OM_EP_LINEDOWN_INFO table' 
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_SALES' 
        ,alias = 'MART_OM_EP_LINEDOWN_INFO'
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

STG_OM_EP_LINEDOWN_INFO AS (
    select
        MD5(OBJECT_CONSTRUCT ('COL1',LindownId::STRING)::STRING ) AS LINEDOWN_INFO_RULE_KEY,
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
        Attribute10
    from {{ source('STG_OUTSYS','STG_OM_EP_LINEDOWN_INFO') }} src 
    where  
    BIW_UPD_DTTM >= '{{V_LWM}}'
    AND BIW_UPD_DTTM < '{{V_HWM}}' 
)

select  LINEDOWN_INFO_RULE_KEY,
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
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ as BIW_UPD_DTTM, 
        '{{V_BIW_BATCH_ID}}' as BIW_BATCH_ID,
        --list of all objects except BIW columns
        md5(object_construct (  'col1',LINEDOWN_INFO_RULE_KEY::string,
                                'col2',LindownId::string,
                                'col3',LDStatus::string,
                                'col4',LDColor::string,
                                'col5',MPN::string,
                                'col6',CPN::string,
                                'col7',ShipTo::string,
                                'col8',ConsumptionCode::string,
                                'col9',EndCustomer::string,
                                'col10',CustomerName::string,
                                'col11',CorporationName::string,
                                'col12',CreatedDate::string,
                                'col13',OpenDays::string,
                                'col14',DropDeadStartDate::string,
                                'col15',DropDeadWeeklyRunRate::string,
                                'col16',DropDeadQuantity::string,
                                'col17',ContractualLiability::string,
                                'col18',CurrentLeadtime::string,
                                'col19',RootcauseForLDEscalation::string,
                                'col20',ConstrainedDevice::string,
                                'col21',ContractCompliance::string,
                                'col22',RecoveryPeriod::string,
                                'col23',CSR::string,
                                'col24',Attribute1::string,
                                'col25',Attribute2::string,
                                'col26',Attribute3::string,
                                'col27',Attribute4::string,
                                'col28',Attribute5::string,
                                'col29',Attribute6::string,
                                'col30',Attribute7::string,
                                'col31',Attribute8::string,
                                'col32',Attribute9::string,
                                'col33',Attribute10::string
                            )
            ::string) as BIW_MD5_KEY  
from STG_OM_EP_LINEDOWN_INFO


