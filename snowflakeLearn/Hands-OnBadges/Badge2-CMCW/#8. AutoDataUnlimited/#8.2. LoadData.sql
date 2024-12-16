--Create a file format and then load each of the 5 Lookup Tables
--You need a file format if you want to load the table
CREATE FILE FORMAT vin.decode.comma_sep_oneheadrow 
type = 'CSV' 
field_delimiter = ',' 
record_delimiter = '\n' 
skip_header = 1 
field_optionally_enclosed_by = '"'  
trim_space = TRUE;

-- Create an AWS External Stage
CREATE OR REPLACE STAGE VIN.DECODE.aws_s3_bucket
    URL = 's3://uni-cmcw/';

-- List files in the S3 Bucket
LIST @VIN.DECODE.aws_s3_bucket;

--Load the Tables and Check Out the Data
COPY INTO vin.decode.wmi_to_manuf
from @vin.decode.aws_s3_bucket
files = ('Maxs_WMIToManuf_data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);

COPY INTO vin.decode.manuf_to_make
from @vin.decode.aws_s3_bucket
files = ('Maxs_ManufToMake_Data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);


COPY INTO vin.decode.model_year
from @vin.decode.aws_s3_bucket
files = ('Maxs_ModelYear_Data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);

--there's a typo in the stage name here. Remember that AWS is case-sensitive and fix the file name
COPY INTO vin.decode.manuf_plants
from @vin.decode.aws_s3_bucket
files = ('Maxs_ManufPlants_Data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);

--there's one table left to load, and one file left to be loaded. 
--figure out what goes in each of the <bracketed> areas to make the final load
COPY INTO vin.decode.make_model_vds
from @vin.decode.aws_s3_bucket
files = ('Maxs_MMVDS_Data.csv')
file_format =(format_name=vin.decode.comma_sep_oneheadrow);






