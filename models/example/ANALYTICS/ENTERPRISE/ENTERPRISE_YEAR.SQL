{{ config(
  description = 'YEAR dimension', 
  materialized = 'view', 
  schema = 'ENTERPRISE', 
  tags = ['MART'],
  alias = 'YEAR'
) }} 

SELECT 
    YEAR_KEY,
    YEAR,
    YEAR_KEY = CAST(YEAR(CURRENT_DATE()) AS INT)	IS_CURRENT_YEAR	,
    YEAR_KEY = CAST(YEAR(CURRENT_DATE()) AS INT) - 1	IS_PRIOR_YEAR	,
    YEAR_DESCRIPTION,
    YEAR_BEGIN_DATE,
    YEAR_END_DATE,
    TOTAL_DAYS_IN_YEAR,
    TOTAL_MONTHS_IN_YEAR,
    TOTAL_QUARTERS_IN_YEAR,
    YEAR_SEQUENCE,
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID	
FROM  {{ref('MART_YEAR')}} 