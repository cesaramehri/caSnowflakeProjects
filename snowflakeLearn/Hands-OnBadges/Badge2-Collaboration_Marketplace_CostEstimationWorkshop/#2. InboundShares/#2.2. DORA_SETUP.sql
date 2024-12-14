-- where did you put the function?
show user functions in account;

-- did you put it here?
select * 
from util_db.information_schema.functions
where function_name = 'GRADER'
and function_catalog = 'UTIL_DB'
and function_owner = 'ACCOUNTADMIN';

--
use role accountadmin;
grant usage 
on function UTIL_DB.PUBLIC.GRADER(VARCHAR, BOOLEAN, NUMBER, NUMBER, VARCHAR) 
to ROLE SYSADMIN;

--
use role sysadmin;
select UTIL_DB.PUBLIC.GRADER(step,(actual = expected), actual, expected, description) as graded_results from (
SELECT 'DORA_IS_WORKING' as step
 ,(select 223 ) as actual
 ,223 as expected
 ,'Dora is working!' as description
);
