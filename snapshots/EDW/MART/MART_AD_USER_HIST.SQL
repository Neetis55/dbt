{% snapshot MART_AD_USER_HIST %}

{{
    config(
       transient=false
      ,target_schema='MART'
      ,alias='AD_USER_HIST'
      ,unique_key = 'USERID'
      ,tags ='MART'
      ,strategy='timestamp'
      ,updated_at='BIW_UPD_DTTM::DATE'
      ,invalidate_hard_deletes=True
      ,merge_update_columns = ['AD_USER_KEY','AD_GROUP', 'DN', 'OBJECTCLASS', 'ONCOMPANYDN', 'EMAILADDRESS', 'ONMODIFIER', 'ONACCOUNTTYPE',
                                'ONSPONSORDN', 'ONPREFERREDNAME', 'SN', 'ONMODIFYTIMESTAMP', 'TELEPHONENUMBER', 'ONDEPARTMENTDN', 'ONDISPLAY',
                                'OTHERSYSTEMS', 'ONMAILDROPDN', 'DISPLAYNAME', 'ONCOMPUTERDN', 'ONSHOWPICTURE', 'GIVENNAME', 'ONLOCATIONDN',
                                'LOGINDISABLED', 'ONSYNCDATE', 'TITLE', 'FACSIMILETELEPHONENUMBER', 'ROOMNUMBER', 'ONSYNCTYPE', 'ONSUPERVISORDN',
                                'ONPURCHASEORDERDN', 'C', 'ONCID', 'ONPAGERDN', 'INITIALS', 'MOBILE', 'ONLICENSEPLATENUMBER', 'BASE', 'HASREPORTS',
                                'DEPARTMENT', 'ADVS_ONCOMPANYDN', 'MANAGER', 'ADVS_ONSPONSORDN', 'ADVS_ONLOCATIONDN', 'ADVS_ONMAILDROPDN',
                                'ONPERSONALPHONE', 'ONPERSONALPAGER', 'ONSSTANUMBER', 'ONBADGEEXPIREDATE', 'ONBADGEISSUEDATE', 'ONBIRTHDATESTR',
                                'DESCRIPTION', 'ONEXTERNALMANAGERNAME', 'ONEXTERNALMANAGERPHONE', 'ONSSN', 'MIRROR_SYNC_TIMESTAMP', 'ONPHONEDN',
                                'ONORACLEPERSONID', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
    )
}}
--Building SCD2 table for Active AD User
--dbt docs requested to do select *
select * from {{ ref('MART_AD_USER') }} 
{% endsnapshot %}