-- Create a Vegetable Details Table
CREATE TABLE GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS(
    PLANT_NAME VARCHAR(25),
    ROOT_DEPTH_CODE VARCHAR(1)
);

-- Insert data by ulpoading the 1st file (comma_opt_enclosed) from file wizard

-- Query the data -- 21 rows
SELECT *
FROM GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS;

-- Insert data by ulpoading the 2nd file (pipe) from file wizard -- 42 rows
SELECT *
FROM GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS;

-- Data Quality checks (spinach has two depth codes)
SELECT *
FROM GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS
WHERE PLANT_NAME = 'Spinach';

-- Data Quality Deduplicating (requirement: only keep shallow 'S' spinach)
DELETE
FROM GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS
WHERE PLANT_NAME = 'Spinach' AND ROOT_DEPTH_CODE = 'D';

-- Data Quality re-check after deduplication
SELECT *
FROM GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS
WHERE PLANT_NAME = 'Spinach';

-- Check the table again, plant_names should be unique now
SELECT *
FROM GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS;
