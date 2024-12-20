--- 📓 Non-Loaded Data is Easy! 
-- 🎯 Run a List Command On the SWEATSUITS Stage
LIST @ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS;



--- 📓 Unstructured Non-Loaded Data 
-- 🥋 Try to Query an Unstructured Data File
SELECT
    $1
FROM
    @ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS/purple_sweatsuit.png;

    

--- 🥋 Querying Non-Loaded Unstructured Data 
-- 🥋 Query with 2 Built-In Meta-Data Columns
SELECT
    metadata$filename, 
    metadata$file_row_number
FROM
    @ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS/purple_sweatsuit.png;

-- 🎯 Write a Query That Returns Something More Like a List Command
SELECT
    metadata$filename, 
    count(metadata$file_row_number)
FROM
    @ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS
GROUP BY
    metadata$filename;



--- 📓 File Formats? Nope, Directory Tables! 
-- 🥋 Query the Directory Table of a Stage (like ls)
SELECT
    *
FROM
    DIRECTORY(@ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS);



--- 📓 Do Functions Work on Directory Tables? 
-- 🥋 Start By Checking Whether Functions will Work on Directory Tables 
SELECT 
    REPLACE(relative_path, '_', ' ') as no_underscores_filename,
    REPLACE(no_underscores_filename, '.png') as just_words_filename,
    INITCAP(just_words_filename) as product_name
FROM 
    DIRECTORY(@ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS);

-- 🎯 Nest 3 Functions into 1 Statement
SELECT 
    INITCAP(REPLACE(REPLACE(relative_path, '_', ' '), '.png')) as product_name
FROM 
    DIRECTORY(@ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS);



--- 🥋 Can you Join Directory Tables with Other Tables? 
-- 🥋 Create an Internal Table in the Zena Database
CREATE OR REPLACE TABLE ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS
(
    color_or_style varchar(25),
	file_name varchar(50),
	price number(5,2)
);

-- fill the new table with some data
insert into  ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS (color_or_style, file_name, price)
values
 ('Burgundy', 'burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'forest_green_sweatsuit.png',64)
,('Navy Blue', 'navy_blue_sweatsuit.png',65)
,('Orange', 'orange_sweatsuit.png',65)
,('Pink', 'pink_sweatsuit.png',63)
,('Purple', 'purple_sweatsuit.png',64)
,('Red', 'red_sweatsuit.png',68)
,('Royal Blue',	'royal_blue_sweatsuit.png',65)
,('Yellow', 'yellow_sweatsuit.png',67);

-- 🎯 Can You Join These?
SELECT
    INITCAP(REPLACE(REPLACE(sd.relative_path, '_', ' '), '.png')) as product_name,
    *
FROM
    DIRECTORY(@ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS) sd
JOIN
    ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS ss
ON
    sd.relative_path = ss.file_name;

    

--- 🥋 3 Way Join - CROSS JOIN Included! 
-- 🎯 Replace the * With a List of Columns
CREATE OR REPLACE VIEW ZENAS_ATHLEISURE_DB.PRODUCTS.PRODUCT_LIST
AS
(
    SELECT
        INITCAP(REPLACE(REPLACE(sd.relative_path, '_', ' '), '.png')) as product_name,
        ss.file_name,
        ss.color_or_style,
        ss.price,
        sd.file_url
    FROM
        DIRECTORY(@ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS) sd
    JOIN
        ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS ss
    ON
        sd.relative_path = ss.file_name
);

-- 🥋 Add the CROSS JOIN
select * from ZENAS_ATHLEISURE_DB.PRODUCTS.PRODUCT_LIST;
select * from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUIT_SIZES;

select * 
from ZENAS_ATHLEISURE_DB.PRODUCTS.PRODUCT_LIST p
cross join ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUIT_SIZES;


-- 🎯 Convert Your Select Statement to a View
CREATE OR REPLACE VIEW ZENAS_ATHLEISURE_DB.PRODUCTS.CATALOG
AS
(
    select 
        *
    from ZENAS_ATHLEISURE_DB.PRODUCTS.PRODUCT_LIST p
    cross join ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUIT_SIZES sz
);

-- check
select * from ZENAS_ATHLEISURE_DB.PRODUCTS.CATALOG;



--- 📓🥋 Do WHAT You Can, WHERE You Can, HOW You Can 
-- 📓 What is a Data Lake?

-- 🥋 Add the Upsell Table and Populate It
-- Add a table to map the sweatsuits to the sweat band sets
create table zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style varchar(25)
,upsell_product_code varchar(10)
);

--populate the upsell table
insert into zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style
,upsell_product_code 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');



--- 📓🥋 Kludges Galore? Or Relentless Resourcefulness? 
-- 🥋 Zena's View for the Athleisure Web Catalog Prototype
-- Zena needs a single view she can query for her website prototype
create view zenas_athleisure_db.products.catalog_for_website as 
select color_or_style
,price
,file_name
, get_presigned_url(@sweatsuits, file_name, 3600) as file_url
,size_list
,coalesce('Consider: ' ||  headband_description || ' & ' || wristband_description, 'Consider: White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, file_name
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from zenas_athleisure_db.products.catalog
    group by color_or_style, price, file_name
) c
left join zenas_athleisure_db.products.upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join zenas_athleisure_db.products.sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join zenas_athleisure_db.products.sweatband_product_line spl
on spl.product_code = sc.product_code;

-- 
select * from zenas_athleisure_db.products.catalog_for_website;
