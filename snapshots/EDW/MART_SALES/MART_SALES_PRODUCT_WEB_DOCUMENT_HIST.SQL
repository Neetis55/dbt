/*---------------------------------------------------------------------------
Command to run model:
dbt run --select ETL_MART_SALES_PRODUCT_WEB_DOCUMENT_HIST

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         06 DEC 2022      VINAYAKA cn      Initial Version
---------------------------------------------------------------------------*/

{% snapshot MART_SALES_PRODUCT_WEB_DOCUMENT_HIST %}
{{
    config(
       transient=false
      ,target_schema='MART_SALES'
      ,alias='PRODUCT_WEB_DOCUMENT_HIST'
      ,unique_key = 'PRODUCT_WEB_DOCUMENT_KEY'
      ,tags ='MART_SALES'
      ,strategy='timestamp'
      ,updated_at='BIW_UPD_DTTM::DATE'
      ,invalidate_hard_deletes=True
      ,merge_update_columns = ['PRODUCT_ID','BASE_PART','DOCUMENT_NAME','DOCUMENT_FILE_NAME','DOCUMENT_TYPE', 'BIW_UPD_DTTM', 'BIW_BATCH_ID','BIW_MD5_KEY']
    )
}}

--dbt docs requested to do select *
select * from {{ ref('MART_SALES_PRODUCT_WEB_DOCUMENT') }} 

{% endsnapshot %}