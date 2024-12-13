------------------------------ Create a New db and set context ------------------------------
USE ROLE SYSADMIN;

-- Create a new db
CREATE DATABASE LIBRARY_CARD_CATALOG
COMMENT = 'DWW Lesson 10 ';

-- Set the Worksheet Context to use the new db
USE DATABASE LIBRARY_CARD_CATALOG;
------------------------------ Create a New db and set context ------------------------------



------------------------------ Create the Book Table ------------------------------
-- Create the book table and use AUTOINCREMENT to generate a UID for each new row
CREATE OR REPLACE TABLE LIBRARY_CARD_CATALOG.PUBLIC.BOOK
(
    BOOK_UID NUMBER AUTOINCREMENT,
    TITLE VARCHAR(50),
    YEAR_PUBLISHED NUMBER(4,0)
);

-- Insert records into the book tabl. You don't have to list anything for the BOOK_UID field because the AUTOINCREMENT property will take care of it for you
INSERT INTO LIBRARY_CARD_CATALOG.PUBLIC.BOOK(TITLE,YEAR_PUBLISHED)
VALUES  ('Food',2001),
        ('Food',2006),
        ('Food',2008),
        ('Food',2016),
        ('Food',2015);

-- Check
SELECT *
FROM LIBRARY_CARD_CATALOG.PUBLIC.BOOK;
------------------------------ Create the Book Table ------------------------------



------------------------------ Create the AUTHOR Table ------------------------------
-- Create the AUTHOR Table
CREATE OR REPLACE TABLE LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR
(
    AUTHOR_UID NUMBER,
    FIRST_NAME VARCHAR(50),
    MIDDLE_NAME VARCHAR(50),
    LAST_NAME VARCHAR(50)
);

-- Insert the first two authors into the Author table
INSERT INTO LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR
VALUES  (1, 'Fiona', '','Macdonald'),
        (2, 'Gian','Paulo','Faleschini');

-- Check
SELECT *
FROM LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR;
------------------------------ Create the AUTHOR Table ------------------------------



------------------------------ Create a Sequence ------------------------------
-- Create sequence for author uid
CREATE OR REPLACE SEQUENCE LIBRARY_CARD_CATALOG.PUBLIC.SEQ_AUTHOR_UID
    START = 1
    INCREMENT = 1
    COMMENT = 'Use this to fill in Author_UID';

-- Check. N.B. Every time you query the sequence, the value will change. 
SELECT  LIBRARY_CARD_CATALOG.PUBLIC.SEQ_AUTHOR_UID.nextval,
        LIBRARY_CARD_CATALOG.PUBLIC.SEQ_AUTHOR_UID.nextval;

-- Check sequences
SHOW SEQUENCES;
------------------------------ Create a Sequence ------------------------------



------------------------------  Recreate the Sequence with a Different Starting Value ------------------------------
-- Drop and recreate the counter (sequence) so that it starts at 3, then we'll add the other author records to our author table
CREATE OR REPLACE SEQUENCE LIBRARY_CARD_CATALOG.PUBLIC.SEQ_AUTHOR_UID
    START = 3
    INCREMENT = 1
    COMMENT = 'Use this to fill in the AUTHOR_UID every time you add a row';

-- Add the remaining author records and use the nextval function instead of putting in the numbers
INSERT INTO LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR(AUTHOR_UID, FIRST_NAME, MIDDLE_NAME, LAST_NAME)
VALUES  (LIBRARY_CARD_CATALOG.PUBLIC.SEQ_AUTHOR_UID.nextval, 'Laura', 'K','Egendorf'),
        (LIBRARY_CARD_CATALOG.PUBLIC.SEQ_AUTHOR_UID.nextval, 'Jan', '','Grover'),
        (LIBRARY_CARD_CATALOG.PUBLIC.SEQ_AUTHOR_UID.nextval, 'Jennifer', '','Clapp'),
        (LIBRARY_CARD_CATALOG.PUBLIC.SEQ_AUTHOR_UID.nextval, 'Kathleen', '','Petelinsek');

-- Check
SELECT * 
FROM LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR;
------------------------------  Recreate the Sequence with a Different Starting Value ------------------------------



------------------------------  Bridge Table: Bridging BOOK <-> AUTHOR ------------------------------
-- Create the BOOK_TO_AUTHOR Bridge Table
CREATE OR REPLACE TABLE LIBRARY_CARD_CATALOG.PUBLIC.BOOK_TO_AUTHOR
(
    BOOK_UID NUMBER,
    AUTHOR_UID NUMBER
);

-- Define relationship
INSERT INTO LIBRARY_CARD_CATALOG.PUBLIC.BOOK_TO_AUTHOR
VALUES  (1,1),
        (1,2),
        (2,3),
        (3,4),
        (4,5),
        (5,6);
    
-- Check
SELECT * 
FROM LIBRARY_CARD_CATALOG.PUBLIC.BOOK_TO_AUTHOR BA
JOIN LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR A
ON BA.AUTHOR_UID = A.AUTHOR_UID
JOIN LIBRARY_CARD_CATALOG.PUBLIC.BOOK B
ON B.BOOK_UID = BA.BOOK_UID;
------------------------------  Bridge Table: Bridging BOOK <-> AUTHOR ------------------------------