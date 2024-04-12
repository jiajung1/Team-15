SELECT
    EMPLOYEE_ID,
    QUIT_DATE,
    _FILE,
    _FIVETRAN_SYNCED AS _FIVETRAN_SYNCED_TS,
    _LINE,
    _MODIFIED AS _MODIFIED_TS
--FROM FIVETRAN_DATABASE.DRIVE_SCHEMA.HR_QUITS
FROM {{source('google_drive', 'HR_QUITS')}}