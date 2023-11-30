{{ 
config(
  description = 'ACTIVE AD USER view for Sales Analytics Team', 
  materialized = 'view', 
  schema = 'SALES', 
  tags = 'MART',
  alias = 'AD_MASTER_USER_RPT'
) 
}} 

SELECT 
    USERID,
    SPLIT_PART(DISPLAYNAME,'|',-1) AS DISPLAYNAME, 
    DEPARTMENT, 
    Value as GROUPNAME,
    TITLE,
    MANAGER,
    EMAILADDRESS, 
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID    
FROM 
{{ref('MART_AD_USER_HIST')}} AD_USER
, lateral strtok_split_to_table(AD_USER.AD_GROUP, ',')
WHERE Value like '%MSTR%'
AND (AD_USER_KEY, DBT_VALID_FROM) IN (SELECT 
                                          AD_USER_KEY,
                                          MAX(DBT_VALID_FROM) 
                                        FROM {{ref('MART_AD_USER_HIST')}}
                                        GROUP BY 1
                                        )