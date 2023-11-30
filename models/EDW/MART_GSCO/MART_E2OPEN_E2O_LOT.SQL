--dbt run --full-refresh --select MART_E2OPEN_E2O_LOT
-- dbt run --select MART_E2OPEN_E2O_LOT

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['LOT_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_E2OPEN_E2O_LOT'-%}
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
         description = 'Building table E2O_LOT dimension for E2OPEN MART '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_E2OPEN'
        ,alias='E2O_LOT'
        ,unique_key= v_pk_list
    	,merge_update_columns =  ['SYSTEM_LOT_NUMBER', 'MES_LOT_NAME', 'LOT_TRANSACTION_ID', 'LOT_ID', 'UPDATED_ON_UDT',
                                'LOCATION', 'LOT_TYPE', 'LOT_CATEGORY', 'LOT_STATE', 'ECAT', 'IS_IN_TRANSIT', 'IS_HOLD', 'IS_REWORK',
                                'IS_MAVERICK_LOT', 'RMA_NUMBER', 'MRB_CASE_NUMBERS', 'EXTERNAL_LOT_NAME', 'IMMEDIATE_PARENT_EXTERNAL_LOT', 'PART_ID',
                                'PDPW', 'STAGE_SET', 'HOLD_COUNT', 'LOT_HOLD_REASON', 'LOT_HOLD_TRANSACTION_UDT', 'SUPPLIER',
                                'SUPPLIER_NAME', 'SUPPLIER_LOCATION', 'SUPPLIER_PO_NUMBER', 'SALES_ORDER_ID', 'SHIPMENT_INVOICE_NUMBER',
                                'SHIPMENT_TRANSFER_PRICE_USED', 'IS_RETURN_SHIPMENT', 'SHIP_TO_CUSTOMER', 'JIT_LOCATION', 'SHELF_RACK', 'SHELF_ROW', 'SHELF_BIN',
                                'WIP_START_UDT', 'WIP_END_UDT', 'PROCESS_PATH', 'HTS_COUNTRY', 'COUNTRY_OF_SUBSTRATE', 'COUNTRY_OF_DIFFUSION',
                                'COUNTRY_OF_FAB', 'FAB_LOCATION', 'FAB_AREA', 'FAB_MES_LOT_NAME', 'FAB_PART_ID', 'FAB_START_UDT', 'FAB_END_UDT',
                                'FAB_DATE_CODE', 'MASK_SET', 'MASK_SET_REVISION', 'DISPOSITION_UDT', 'COUNTRY_OF_SORT', 'SORT_LOCATION',
                                'SORT_AREA', 'SORT_MES_LOT_NAME', 'SORT_PART_ID', 'INK_UDT', 'SORT_START_UDT', 'SORT_END_UDT',
                                'SORT_PROGRAM_NAME', 'SORT_PROGRAM_REVISION', 'SORT_PROGRAM_CHECKSUM', 'SORT_TESTER_ID', 'SORT_PROBER_ID',
                                'SORT_PROBE_CARD', 'SORT_LOADBOARD_ID', 'SORT_DUT_BOARD_ID', 'COUNTRY_OF_BUMP', 'BUMP_LOCATION',
                                'COUNTRY_OF_ASSEMBLY', 'ASSEMBLY_LOCATION', 'ASSEMBLY_AREA', 'ASSEMBLY_MES_LOT_NAME', 'ASSEMBLY_PART_ID',
                                'ASSEMBLY_START_UDT', 'ASSEMBLY_END_UDT', 'DATE_CODE', 'DATE2_CODE', 'IS_MARKED', 'TOP_SIDE_MARK', 'BACK_SIDE_MARK',
                                'TRACE_CODE', 'TRACE2_CODE', 'COUNTRY_OF_FINAL_TEST', 'FINAL_TEST_LOCATION', 'FINAL_TEST_AREA',
                                'FINAL_TEST_MES_LOT_NAME', 'FINAL_TEST_PART_ID', 'FINAL_TEST_START_UDT', 'FINAL_TEST_END_UDT', 'FINAL_TEST_TEMP',
                                'FINAL_TEST_PROGRAM_NAME', 'FINAL_TEST_PROGRAM_REVISION', 'FINAL_TEST_PROGRAM_CHECKSUM', 'FINAL_TEST_TESTER_ID',
                                'FINAL_TEST_HANDLER_ID', 'FINAL_TEST_LOADBOARD_ID', 'FINAL_TEST_DUT_BOARD_ID', 'DRY_PACK_SEAL_UDT', 'SEAL_OPEN_UDT',
                                'TARGET_PART_ID', 'SCHEDULED_START_UDT', 'PROJECTED_OUT_UDT', 'PROJECTED_OUT_WW',
                                'MOST_RECENT_MOVE_TRANSACTION_UDT', 'LOCATION_MOVE_IN_MES_TRANSACTION_UDT', 'FROM_LOT_ID', 'FROM_MES_LOT_NAME', 'TO_MES_LOT_NAME',
                                'PREVIOUS_MES_LOT_NAME', 'NEW_MES_LOT_NAME', 'BOH_LOT_TYPE', 'EOH_LOT_TYPE', 'BOH_PART_ID', 'EOH_PART_ID', 'PACKAGE_TYPE',
                                'EXTERNAL_PART_ID', 'FROM_LOT_PART_ID', 'BOH_STAGE_SET', 'EOH_STAGE_SET', 'IS_VALUE', 'ASSIGNED_TO', 'IS_ACTIVE',
                                'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
		,tags =['E2OPEN']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}
WITH STG_LOT_DTL AS
(
    SELECT 
    MD5(SYSTEM_LOT_NUMBER) AS LOT_KEY
    ,SYSTEM_LOT_NUMBER
    ,MES_LOT_NAME
    ,LOT_TRANSACTION_ID
    ,LOT_ID
    ,UPDATED_ON_UDT
    ,LOCATION
    ,LOT_TYPE
    ,LOT_CATEGORY
    ,LOT_STATE
    ,ECAT
    ,IS_IN_TRANSIT
    ,IS_HOLD
    ,IS_REWORK
    ,IS_MAVERICK_LOT
    ,RMA_NUMBER
    ,MRB_CASE_NUMBERS
    ,EXTERNAL_LOT_NAME
    ,IMMEDIATE_PARENT_EXTERNAL_LOT
    ,PART_ID
    ,PDPW
    ,STAGE_SET
    ,HOLD_COUNT
    ,LOT_HOLD_REASON
    ,LOT_HOLD_TRANSACTION_UDT
    ,SUPPLIER
    ,SUPPLIER_NAME
    ,SUPPLIER_LOCATION
    ,SUPPLIER_PO_NUMBER
    ,SALES_ORDER_ID
    ,SHIPMENT_INVOICE_NUMBER
    ,SHIPMENT_TRANSFER_PRICE_USED
    ,IS_RETURN_SHIPMENT
    ,SHIP_TO_CUSTOMER
    ,JIT_LOCATION
    ,SHELF_RACK
    ,SHELF_ROW
    ,SHELF_BIN
    ,WIP_START_UDT
    ,WIP_END_UDT
    ,PROCESS_PATH
    ,HTS_COUNTRY
    ,COUNTRY_OF_SUBSTRATE
    ,COUNTRY_OF_DIFFUSION
    ,COUNTRY_OF_FAB
    ,FAB_LOCATION
    ,FAB_AREA
    ,FAB_MES_LOT_NAME
    ,FAB_PART_ID
    ,FAB_START_UDT
    ,FAB_END_UDT
    ,FAB_DATE_CODE
    ,MASK_SET
    ,MASK_SET_REVISION
    ,DISPOSITION_UDT
    ,COUNTRY_OF_SORT
    ,SORT_LOCATION
    ,SORT_AREA
    ,SORT_MES_LOT_NAME
    ,SORT_PART_ID
    ,INK_UDT
    ,SORT_START_UDT
    ,SORT_END_UDT
    ,SORT_PROGRAM_NAME
    ,SORT_PROGRAM_REVISION
    ,SORT_PROGRAM_CHECKSUM
    ,SORT_TESTER_ID
    ,SORT_PROBER_ID
    ,SORT_PROBE_CARD
    ,SORT_LOADBOARD_ID
    ,SORT_DUT_BOARD_ID
    ,COUNTRY_OF_BUMP
    ,BUMP_LOCATION
    ,COUNTRY_OF_ASSEMBLY
    ,ASSEMBLY_LOCATION
    ,ASSEMBLY_AREA
    ,ASSEMBLY_MES_LOT_NAME
    ,ASSEMBLY_PART_ID
    ,ASSEMBLY_START_UDT
    ,ASSEMBLY_END_UDT
    ,DATE_CODE
    ,DATE2_CODE
    ,IS_MARKED
    ,TOP_SIDE_MARK
    ,BACK_SIDE_MARK
    ,TRACE_CODE
    ,TRACE2_CODE
    ,COUNTRY_OF_FINAL_TEST
    ,FINAL_TEST_LOCATION
    ,FINAL_TEST_AREA
    ,FINAL_TEST_MES_LOT_NAME
    ,FINAL_TEST_PART_ID
    ,FINAL_TEST_START_UDT
    ,FINAL_TEST_END_UDT
    ,FINAL_TEST_TEMP
    ,FINAL_TEST_PROGRAM_NAME
    ,FINAL_TEST_PROGRAM_REVISION
    ,FINAL_TEST_PROGRAM_CHECKSUM
    ,FINAL_TEST_TESTER_ID
    ,FINAL_TEST_HANDLER_ID
    ,FINAL_TEST_LOADBOARD_ID
    ,FINAL_TEST_DUT_BOARD_ID
    ,DRY_PACK_SEAL_UDT
    ,SEAL_OPEN_UDT
    ,TARGET_PART_ID
    ,SCHEDULED_START_UDT
    ,PROJECTED_OUT_UDT
    ,PROJECTED_OUT_WW
    ,MOST_RECENT_MOVE_TRANSACTION_UDT
    ,LOCATION_MOVE_IN_MES_TRANSACTION_UDT
    ,FROM_LOT_ID
    ,FROM_MES_LOT_NAME
    ,TO_MES_LOT_NAME
    ,PREVIOUS_MES_LOT_NAME
    ,NEW_MES_LOT_NAME
    ,BOH_LOT_TYPE
    ,EOH_LOT_TYPE
    ,BOH_PART_ID
    ,EOH_PART_ID
    ,PACKAGE_TYPE
    ,EXTERNAL_PART_ID
    ,FROM_LOT_PART_ID
    ,BOH_STAGE_SET
    ,EOH_STAGE_SET
    ,IS_VALUE
    ,ASSIGNED_TO
    ,IS_ACTIVE
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM 
    ,'{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM 
    ,{{V_BIW_BATCH_ID}} as BIW_BATCH_ID  
    ,md5(object_construct ('col1',SYSTEM_LOT_NUMBER, 'col2',MES_LOT_NAME, 'col3',LOT_TRANSACTION_ID, 'col4',LOT_ID,
    'col5',UPDATED_ON_UDT, 'col6',LOCATION, 'col7',LOT_TYPE, 'col8',LOT_CATEGORY, 'col9',LOT_STATE, 'col10',ECAT, 'col11',IS_IN_TRANSIT,
    'col12',IS_HOLD, 'col13',IS_REWORK, 'col14',IS_MAVERICK_LOT, 'col15',RMA_NUMBER, 'col16',MRB_CASE_NUMBERS, 'col17',EXTERNAL_LOT_NAME,
    'col18',IMMEDIATE_PARENT_EXTERNAL_LOT, 'col19',PART_ID, 'col20',PDPW, 'col21',STAGE_SET, 'col22',HOLD_COUNT, 'col23',LOT_HOLD_REASON,
    'col24',LOT_HOLD_TRANSACTION_UDT, 'col25',SUPPLIER, 'col26',SUPPLIER_NAME, 'col27',SUPPLIER_LOCATION, 'col28',SUPPLIER_PO_NUMBER, 'col29',SALES_ORDER_ID,
    'col30',SHIPMENT_INVOICE_NUMBER, 'col31',SHIPMENT_TRANSFER_PRICE_USED, 'col32',IS_RETURN_SHIPMENT, 'col33',SHIP_TO_CUSTOMER, 'col34',JIT_LOCATION,
    'col35',SHELF_RACK, 'col36',SHELF_ROW, 'col37',SHELF_BIN, 'col38',WIP_START_UDT, 'col39',WIP_END_UDT, 'col40',PROCESS_PATH,
    'col41',HTS_COUNTRY, 'col42',COUNTRY_OF_SUBSTRATE, 'col43',COUNTRY_OF_DIFFUSION, 'col44',COUNTRY_OF_FAB, 'col45',FAB_LOCATION, 'col46',FAB_AREA,
    'col47',FAB_MES_LOT_NAME, 'col48',FAB_PART_ID, 'col49',FAB_START_UDT, 'col50',FAB_END_UDT, 'col51',FAB_DATE_CODE, 'col52',MASK_SET,
    'col53',MASK_SET_REVISION, 'col54',DISPOSITION_UDT, 'col55',COUNTRY_OF_SORT, 'col56',SORT_LOCATION, 'col57',SORT_AREA, 'col58',SORT_MES_LOT_NAME,
    'col59',SORT_PART_ID, 'col60',INK_UDT, 'col61',SORT_START_UDT, 'col62',SORT_END_UDT, 'col63',SORT_PROGRAM_NAME, 'col64',SORT_PROGRAM_REVISION,
    'col65',SORT_PROGRAM_CHECKSUM, 'col66',SORT_TESTER_ID, 'col67',SORT_PROBER_ID, 'col68',SORT_PROBE_CARD, 'col69',SORT_LOADBOARD_ID,
    'col70',SORT_DUT_BOARD_ID, 'col71',COUNTRY_OF_BUMP, 'col72',BUMP_LOCATION, 'col73',COUNTRY_OF_ASSEMBLY, 'col74',ASSEMBLY_LOCATION,
    'col75',ASSEMBLY_AREA, 'col76',ASSEMBLY_MES_LOT_NAME, 'col77',ASSEMBLY_PART_ID, 'col78',ASSEMBLY_START_UDT, 'col79',ASSEMBLY_END_UDT,
    'col80',DATE_CODE, 'col81',DATE2_CODE, 'col82',IS_MARKED, 'col83',TOP_SIDE_MARK, 'col84',BACK_SIDE_MARK, 'col85',TRACE_CODE,
    'col86',TRACE2_CODE, 'col87',COUNTRY_OF_FINAL_TEST, 'col88',FINAL_TEST_LOCATION, 'col89',FINAL_TEST_AREA, 'col90',FINAL_TEST_MES_LOT_NAME,
    'col91',FINAL_TEST_PART_ID, 'col92',FINAL_TEST_START_UDT, 'col93',FINAL_TEST_END_UDT, 'col94',FINAL_TEST_TEMP, 'col95',FINAL_TEST_PROGRAM_NAME,
    'col96',FINAL_TEST_PROGRAM_REVISION, 'col97',FINAL_TEST_PROGRAM_CHECKSUM, 'col98',FINAL_TEST_TESTER_ID, 'col99',FINAL_TEST_HANDLER_ID,
    'col100',FINAL_TEST_LOADBOARD_ID, 'col101',FINAL_TEST_DUT_BOARD_ID, 'col102',DRY_PACK_SEAL_UDT, 'col103',SEAL_OPEN_UDT, 'col104',TARGET_PART_ID,
    'col105',SCHEDULED_START_UDT, 'col106',PROJECTED_OUT_UDT, 'col107',PROJECTED_OUT_WW, 'col108',MOST_RECENT_MOVE_TRANSACTION_UDT,
    'col109',LOCATION_MOVE_IN_MES_TRANSACTION_UDT, 'col110',FROM_LOT_ID, 'col111',FROM_MES_LOT_NAME, 'col112',TO_MES_LOT_NAME, 'col113',PREVIOUS_MES_LOT_NAME,
    'col114',NEW_MES_LOT_NAME, 'col115',BOH_LOT_TYPE, 'col116',EOH_LOT_TYPE, 'col117',BOH_PART_ID, 'col118',EOH_PART_ID, 'col119',PACKAGE_TYPE,
    'col120',EXTERNAL_PART_ID, 'col121',FROM_LOT_PART_ID, 'col122',BOH_STAGE_SET, 'col123',EOH_STAGE_SET, 'col124',IS_VALUE, 'col125',ASSIGNED_TO,
    'col126',IS_ACTIVE)::string ) as BIW_MD5_KEY 
    FROM 
    {{ref('ETL_MART_E2OPEN_E2O_LOT_DTL')}}
    {% if is_incremental() %}
    WHERE BIW_UPD_DTTM >= '{{V_LWM}}'
    AND BIW_UPD_DTTM < '{{V_HWM}}'
    {% endif %}
)
SELECT 
STG.* 
FROM STG_LOT_DTL STG 
{% if is_incremental() %}
    LEFT JOIN {{ this }} TGT
    on STG.LOT_KEY= TGT.LOT_KEY
    WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY OR TGT.BIW_MD5_KEY IS NULL
{% endif %}