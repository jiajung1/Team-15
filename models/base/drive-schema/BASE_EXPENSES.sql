SELECT 
    _FILE,
    _LINE,
    _MODIFIED AS _MODIEFIED_TS,
    _FIVETRAN_SYNCED AS _FIVETRAN_SYNCED_TS,
    "DATE",
    EXPENSE_TYPE,
    CAST(REPLACE(EXPENSE_AMOUNT, '$ ', '') AS DECIMAL) AS EXPENSE_AMOUNT
FROM {{source('google_drive', 'EXPENSES')}}
--FROM FIVETRAN_DATABASE.DRIVE_SCHEMA.EXPENSES