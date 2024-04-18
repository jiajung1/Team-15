SELECT 
    bhr.EMPLOYEE_ID,
    NAME,
    HIRE_DATE,
    QUIT_DATE,
    CITY,
    ADDRESS,
    TITLE,
    ANNUAL_SALARY,
    CASE WHEN QUIT_DATE IS null then 0 else 1 END AS STILL_IN_COMPANY,
FROM {{ref('BASE_HR_JOINS')}} bhr
LEFT JOIN {{ref('BASE_HR_QUITS')}} bhq
ON bhr.EMPLOYEE_ID = bhq.EMPLOYEE_ID