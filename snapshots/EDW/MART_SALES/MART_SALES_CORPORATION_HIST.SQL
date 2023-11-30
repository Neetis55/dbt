
{% snapshot MART_SALES_CORPORATION_HIST %}

{{
    config(
       transient=false
      ,target_schema='MART_SALES'
      ,alias='CORPORATION_HIST'
      ,unique_key = 'CORPORATION_KEY'
      ,tags ='MART_SALES'
      ,strategy='timestamp'
      ,updated_at='BIW_UPD_DTTM::DATE'
      ,invalidate_hard_deletes=True
      ,merge_update_columns = ['CORPORATION_CODE', 'CORPORATION_DESCRIPTION', 'ERP_CUST_ACCOUNT_ID', 'CORPORATION_CATEGORY_CODE'
                            , 'CORPORATION_CATEGORY_DESCRIPTION', 'ERP_STATUS_CODE', 'HOME_REGION', 'IS_MARKET_MAKER', 'CLASS'
                            , 'URL', 'ACCOUNT_TYPE', 'IS_CUSTOMER_ITEM_CONTROL_REQUIRED', 'BARCODE_LABEL_CODE', 'VENDOR_CODE'
                            , 'COLLECTOR_NAME', 'TAX_PRINTING_DESCRIPTION', 'GROUPING_RULE_CODE', 'HAS_CREDIT_CHECK', 'HAS_CREDIT_HOLD'
                            , 'CONTRACT_NAME', 'GAM_USER_ID', 'GAM_FIRST_NAME', 'GAM_LAST_NAME', 'GAM_EMAIL', 'GAM_PHONE'
                            , 'GAM_CONTACT_LAST_UPDATE_BY', 'GAM_CONTACT_CREATED_BY', 'GSM_USER_ID', 'GSM_FIRST_NAME', 'GSM_LAST_NAME'
                            , 'GSM_EMAIL', 'GSM_PHONE', 'GSM_CONTACT_LAST_UPDATE_BY', 'GSM_CONTACT_CREATED_BY'
                            , 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
    )
}}
--Building SCD2 table for Sales Territory
--dbt docs requested to do select *
select * from {{ ref('MART_SALES_CORPORATION') }} 

{% endsnapshot %}