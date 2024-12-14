-- Rename a DB
ALTER DATABASE SAMPLE_DATA_NEW
RENAME TO SNOWFLAKE_SAMPLE_DATA;

-- Grant privileges on DB
GRANT imported PRIVILEGES
ON DATABASE SNOWFLAKE_SAMPLE_DATA
TO ROLE SYSADMIN;



------------------------------  Use Select Statements to Look at Sample Data ------------------------------
-- Check the range of values in the Market Segment Column
SELECT DISTINCT c_mktsegment
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

--Find out which Market Segments have the most customers
SELECT      c_mktsegment,
            COUNT(*)
FROM        SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
GROUP BY    c_mktsegment
ORDER BY    COUNT(*);
------------------------------  Use Select Statements to Look at Sample Data ------------------------------



------------------------------  Join and Aggregate Shared Data ------------------------------
-- Nations Table
SELECT  N_NATIONKEY,
        N_NAME,
        N_REGIONKEY
FROM    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;

-- Regions Table
SELECT  R_REGIONKEY,
        R_NAME
FROM    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

-- Join the Tables and Sort
SELECT      R.R_NAME AS REGION,
            N.N_NAME AS NATION
FROM        SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION N
JOIN        SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION R
ON          N.N_REGIONKEY = R.R_REGIONKEY
ORDER BY    R.R_NAME, N.N_NAME ASC;

--Group and Count Rows Per Region
SELECT      R.R_NAME AS REGION,
            COUNT(N.N_NAME) AS NUM_NATIONS
FROM        SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION N
JOIN        SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION R
ON          N.N_REGIONKEY = R.R_REGIONKEY
GROUP BY    R.R_NAME;
------------------------------  Join and Aggregate Shared Data ------------------------------



------------------------------  Export Native and Shared Data ------------------------------
--The real value of consuming shared data is:

--Someone else will maintain it over time and keep it fresh
--Someone else will pay to store it
--You will only pay to query it
------------------------------  Export Native and Shared Data ------------------------------
