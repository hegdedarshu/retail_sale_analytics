CREATE DATABASE RETAIL_STAGE_DB;

CREATE SCHEMA RETAIL_STAGE_DB.RAW;
CREATE SCHEMA RETAIL_STAGE_DB.DW;
CREATE SCHEMA RETAIL_STAGE_DB.SEMANTIC;
CREATE SCHEMA RETAIL_STAGE_DB.OPS;
CREATE SCHEMA RETAIL_DB.QUARANTINE;



CREATE TABLE RETAIL_DB.RAW.RAW_CUSTOMERS (
    CUSTOMER_ID     VARCHAR,
    FIRST_NAME      VARCHAR,
    LAST_NAME       VARCHAR,
    EMAIL           VARCHAR,
    PHONE           VARCHAR,
    SEGMENT         VARCHAR,
    CITY            VARCHAR,
    STATE           VARCHAR,
    COUNTRY         VARCHAR,
    UPDATED_AT      TIMESTAMP,
    SRC_FILE_NAME   VARCHAR,
    LOAD_TS         TIMESTAMP
);


CREATE OR REPLACE STORAGE INTEGRATION retail_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::498504718091:role/dbdarsh_role'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://bktdbdarsh/capstone/'
  );

  DESC STORAGE INTEGRATION retail_s3_int;


  CREATE OR REPLACE FILE FORMAT RETAIL_DB.RAW.CUSTOMER_CSV_FF
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO';

  CREATE OR REPLACE STAGE RETAIL_DB.RAW.CUSTOMER_STAGE
  URL = 's3://bktdbdarsh/capstone/customers/'
  STORAGE_INTEGRATION = retail_s3_int
  FILE_FORMAT = RETAIL_DB.RAW.CUSTOMER_CSV_FF;

  list @RETAIL_DB.RAW.CUSTOMER_STAGE;



CREATE OR REPLACE PIPE RETAIL_DB.RAW.PIPE_CUSTOMERS
  AUTO_INGEST = TRUE
AS
COPY INTO RETAIL_DB.RAW.RAW_CUSTOMERS
(
  CUSTOMER_ID,
  FIRST_NAME,
  LAST_NAME,
  EMAIL,
  PHONE,
  SEGMENT,
  CITY,
  STATE,
  COUNTRY,
  UPDATED_AT,
  SRC_FILE_NAME,
  LOAD_TS
)
FROM (
    SELECT
        $1,                         -- CUSTOMER_ID
        $2,                         -- FIRST_NAME
        $3,                         -- LAST_NAME
        $4,                         -- EMAIL
        $5,                         -- PHONE
        $6,                         -- SEGMENT
        $7,                         -- CITY
        $8,                         -- STATE
        $9,                         -- COUNTRY
        TO_TIMESTAMP($10),          -- UPDATED_AT
        METADATA$FILENAME,          -- source file name
        CURRENT_TIMESTAMP()         -- load timestamp
    FROM @RETAIL_DB.RAW.CUSTOMER_STAGE
)
ON_ERROR = 'CONTINUE';

show pipes;

select * from RETAIL_DB.RAW.RAW_CUSTOMERS;

-- ===================================================================================

CREATE OR REPLACE TABLE RETAIL_DB.RAW.RAW_PRODUCTS (
    PRODUCT_ID     VARCHAR,
    PRODUCT_NAME   VARCHAR,
    CATEGORY       VARCHAR,
    SUBCATEGORY    VARCHAR,
    BRAND          VARCHAR,
    UNIT_PRICE     NUMBER(10,2),
    STATUS         VARCHAR,
    UPDATED_AT     TIMESTAMP,
    SRC_FILE_NAME  VARCHAR,
    LOAD_TS        TIMESTAMP
);

CREATE OR REPLACE STAGE RETAIL_DB.RAW.PRODUCT_STAGE
  URL = 's3://bktdbdarsh/capstone/products/'
  STORAGE_INTEGRATION = retail_s3_int
  FILE_FORMAT = RETAIL_DB.RAW.CUSTOMER_CSV_FF;


  CREATE OR REPLACE PIPE RETAIL_DB.RAW.PIPE_PRODUCTS
  AUTO_INGEST = TRUE
AS
COPY INTO RETAIL_DB.RAW.RAW_PRODUCTS
(
  PRODUCT_ID,
  PRODUCT_NAME,
  CATEGORY,
  SUBCATEGORY,
  BRAND,
  UNIT_PRICE,
  STATUS,
  UPDATED_AT,
  SRC_FILE_NAME,
  LOAD_TS
)
FROM (
    SELECT
        $1,                        -- PRODUCT_ID
        $2,                        -- PRODUCT_NAME
        $3,                        -- CATEGORY
        $4,                        -- SUBCATEGORY
        $5,                        -- BRAND
        TO_NUMBER($6, 10, 2),      -- UNIT_PRICE
        $7,                        -- STATUS
        TO_TIMESTAMP($8),          -- UPDATED_AT
        METADATA$FILENAME,         -- source file
        CURRENT_TIMESTAMP()        -- load timestamp
    FROM @RETAIL_DB.RAW.PRODUCT_STAGE
)
ON_ERROR = 'CONTINUE';

truncate table RETAIL_DB.RAW.RAW_PRODUCTS;

select * from RETAIL_DB.RAW.RAW_PRODUCTS;

-- ============================================================================================================


CREATE OR REPLACE TABLE RETAIL_DB.RAW.RAW_SALES (
    ORDER_ID        VARCHAR,
    ORDER_LINE_ID   NUMBER,
    ORDER_DATE      DATE,
    CUSTOMER_ID     VARCHAR,
    PRODUCT_ID      VARCHAR,
    STORE_ID        VARCHAR,
    QTY             NUMBER,
    UNIT_PRICE      NUMBER(10,2),
    DISCOUNT_PCT    NUMBER(5,2),
    PAYMENT_TYPE    VARCHAR,
    ORDER_STATUS    VARCHAR,
    SRC_FILE_NAME   VARCHAR,
    LOAD_TS         TIMESTAMP
);



CREATE OR REPLACE STAGE RETAIL_DB.RAW.SALE_STAGE
  URL = 's3://bktdbdarsh/capstone/sales/'
  STORAGE_INTEGRATION = retail_s3_int
  FILE_FORMAT = RETAIL_DB.RAW.CUSTOMER_CSV_FF;


CREATE OR REPLACE PIPE RETAIL_DB.RAW.PIPE_SALES
  AUTO_INGEST = TRUE
AS
COPY INTO RETAIL_DB.RAW.RAW_SALES
(
  ORDER_ID,
  ORDER_LINE_ID,
  ORDER_DATE,
  CUSTOMER_ID,
  PRODUCT_ID,
  STORE_ID,
  QTY,
  UNIT_PRICE,
  DISCOUNT_PCT,
  PAYMENT_TYPE,
  ORDER_STATUS,
  SRC_FILE_NAME,
  LOAD_TS
)
FROM (
    SELECT
        $1,                         -- ORDER_ID
        TO_NUMBER($2),              -- ORDER_LINE_ID
        TO_DATE($3),                -- ORDER_DATE
        $4,                         -- CUSTOMER_ID
        $5,                         -- PRODUCT_ID
        $6,                         -- STORE_ID
        TO_NUMBER($7),              -- QTY
        TO_NUMBER($8, 10, 2),        -- UNIT_PRICE
        TO_NUMBER($9, 5, 2),         -- DISCOUNT_PCT
        $10,                        -- PAYMENT_TYPE
        $11,                        -- ORDER_STATUS
        METADATA$FILENAME,          -- source file
        CURRENT_TIMESTAMP()         -- load timestamp
    FROM @RETAIL_DB.RAW.SALE_STAGE
)
ON_ERROR = 'CONTINUE';

truncate table RETAIL_DB.RAW.RAW_SALES;

select * from  RETAIL_DB.RAW.RAW_SALES;

-- ===================================================

CREATE OR REPLACE TABLE RETAIL_DB.RAW.RAW_STORES (
    STORE_ID       VARCHAR,
    STORE_NAME     VARCHAR,
    REGION         VARCHAR,
    CITY           VARCHAR,
    STATE          VARCHAR,
    COUNTRY        VARCHAR,
    OPEN_DATE      DATE,
    STATUS         VARCHAR,
    SRC_FILE_NAME  VARCHAR,
    LOAD_TS        TIMESTAMP
);



CREATE OR REPLACE STAGE RETAIL_DB.RAW.STORE_STAGE
  URL = 's3://bktdbdarsh/capstone/stores/'
  STORAGE_INTEGRATION = retail_s3_int
  FILE_FORMAT = RETAIL_DB.RAW.CUSTOMER_CSV_FF;

  list @RETAIL_DB.RAW.STORE_STAGE;


  CREATE OR REPLACE PIPE RETAIL_DB.RAW.PIPE_STORES
  AUTO_INGEST = TRUE
AS
COPY INTO RETAIL_DB.RAW.RAW_STORES
(
  STORE_ID,
  STORE_NAME,
  REGION,
  CITY,
  STATE,
  COUNTRY,
  OPEN_DATE,
  STATUS,
  SRC_FILE_NAME,
  LOAD_TS
)
FROM (
    SELECT
        $1,                         -- STORE_ID
        $2,                         -- STORE_NAME
        $3,                         -- REGION
        $4,                         -- CITY
        $5,                         -- STATE
        $6,                         -- COUNTRY
        TO_DATE($7),                -- OPEN_DATE
        $8,                         -- STATUS
        METADATA$FILENAME,          -- source file
        CURRENT_TIMESTAMP()         -- load timestamp
    FROM @RETAIL_DB.RAW.STORE_STAGE
)
ON_ERROR = 'CONTINUE';

truncate table RETAIL_DB.RAW.RAW_STORES;


select * from RETAIL_DB.RAW.RAW_STORES;


-- =================================================================

-- dimension and lookup 
CREATE OR REPLACE TABLE DW.DIM_DATE (
  SK_DATE        NUMBER AUTOINCREMENT START 1 INCREMENT 1,   -- PK surrogate
  DATE_VALUE     DATE        NOT NULL,                       -- UK (actual date)

  YEAR           NUMBER      NOT NULL,
  QUARTER        NUMBER      NOT NULL,
  MONTH          NUMBER      NOT NULL,
  WEEK_OF_YEAR   NUMBER      NOT NULL,
  IS_WEEKEND     STRING(1)   NOT NULL,                       -- 'Y'/'N'

  LOAD_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT PK_DIM_DATE PRIMARY KEY (SK_DATE),
  CONSTRAINT UK_DIM_DATE UNIQUE (DATE_VALUE)
);

 show tables;
 
select * from dw.dim_date; 
 drop table retail_db.dw.DIM_DATE;


 show tables;

truncate table dim_date;


select * from dim_date;




CREATE OR REPLACE TABLE RETAIL_DB.DW.DIM_STORE (
    SK_STORE    NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    STORE_ID    VARCHAR(50) NOT NULL,
    STORE_NAME  VARCHAR(200),
    REGION      VARCHAR(50),
    CITY        VARCHAR(100),
    STATE       VARCHAR(50),
    COUNTRY     VARCHAR(50),
    OPEN_DATE   DATE,
    STATUS      VARCHAR(20),
    CONSTRAINT PK_DIM_STORE PRIMARY KEY (SK_STORE)
);


SHOW TABLES;

drop table retail_db.dw.dim_store;

SELECT * FROM DIM_2;



CREATE OR REPLACE TABLE RETAIL_DB.QUARANTINE.DIM_STORE (
    SK_STORE    NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    STORE_ID    VARCHAR(50) NOT NULL,
    STORE_NAME  VARCHAR(200),
    REGION      VARCHAR(50),
    CITY        VARCHAR(100),
    STATE       VARCHAR(50),
    COUNTRY     VARCHAR(50),
    OPEN_DATE   DATE,
    STATUS      VARCHAR(20),
    CONSTRAINT PK_DIM_STORE PRIMARY KEY (SK_STORE)
);

SELECT * FROM DIM_store;

truncate table DIM_STORE;


show tables;

UPDATE RETAIL_DB.QUARANTINE.DIM_2
SET STATE='MH'
WHERE STORE_ID = 'S00002';

show tables;


select * from dim_2;


INSERT INTO RETAIL_DB.QUARANTINE.DIM_2
(
  FLAG_,
  STORE_ID,
  STORE_NAME,
  REGION,
  CITY,
  STATE,
  COUNTRY,
  OPEN_DATE,
  STATUS,
  SRC_FILE_NAME,
  LOAD_TS
)
VALUES
(
  'N',
  'S1210001',
  'VISHAL_MART',
  'SOUTH',
  'HUBLI',
  'KAR',
  'India',
  TO_TIMESTAMP('2020-02-09 00:00:00','YYYY-MM-DD HH24:MI:SS'),
  'RENOVATION',
  'capstone/stores/',
  TO_TIMESTAMP('2026-02-03 09:25:29','YYYY-MM-DD HH24:MI:SS')
);

commit;



SHOW TABLES;



SHOW TABLES;


select * from raw_products;



select * from dim_pro;

select * from dim_customer;


show tables;


select * from dim_2;

update dim_2 set region='east' where store_id='S00001';

COMMIT;

truncate table dim_2;


SELECT STORE_ID, COUNT(*)
FROM RETAIL_DB.quarantine.DIM_2
GROUP BY STORE_ID
HAVING COUNT(*) > 1;


show tables;

select * from dim_store;

truncate table dim_store;


select * from dim_store;


update dim_2 set state='DELHI' where store_id='S00002';

commit;

rollback;


show tables;

select 

