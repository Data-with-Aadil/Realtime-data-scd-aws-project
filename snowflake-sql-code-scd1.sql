create database if not exists scd_demo;
use database scd_demo;
create schema if not exists scd2;
use schema scd2;
show tables;

-- raw staging table:-(staging table) 
-- whenever we have any new data onto our s3 bucket, that data will be loaded to this table.
-- then using the snowflake stream and snowflake task --> we will update customer history and customer table.
-- all the csv file data will be appended to the raw table.
create or replace table customer_raw (
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar);



-- customer_table:- (production table) 
-- it will keep track of all the updates of each and every update that is been made to the actual table.
create or replace table customer (
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar,
     update_timestamp timestamp_ntz default current_timestamp());
     
-- customer history table :- scd-2 tables.
-- all the history of the data will be here.

create or replace table customer_history (
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar,
     start_time timestamp_ntz default current_timestamp(),
     end_time timestamp_ntz default current_timestamp(),
     is_current boolean
     );

show tables;

-- now we will create a stream on the customer table so that we will keep a track of it and updte the same in the history table.

create or replace stream customer_table_changes on table customer;


-------------------------------------------------------------------------------------------------------------------------------

-- now let's load our data:-
// Creating external stage (create your own bucket)
CREATE OR REPLACE STAGE SCD_DEMO.SCD2.customer_ext_stage
    url='s3://snowflake-realtime-raw-data/fakedata/'
    credentials=(aws_key_id='AKIASH6LO7PK5BHR4A7L' aws_secret_key='zYRWyfWP25LcbGhrXYv86X5yKh7EtsLgwDzVr1yy');

-- csv file format:-
CREATE OR REPLACE FILE FORMAT SCD_DEMO.SCD2.CSV
TYPE = CSV,
FIELD_DELIMITER = ","
SKIP_HEADER = 1;

SHOW STAGES;

--list all files available on the s3
LIST @customer_ext_stage;

-- before create a pipe, let's try to check copy command working or not.
COPY INTO customer_raw
FROM @customer_ext_stage/customer_20231112060115.csv
FILE_FORMAT = CSV

select * from customer_raw;

-- now let's create a pipe on s3.
CREATE OR REPLACE PIPE customer_s3_pipe
  auto_ingest = true
  AS
  COPY INTO customer_raw
  FROM @customer_ext_stage
  FILE_FORMAT = CSV
  ;

show pipes;

-- now copy the notification channel and create a notification channel on s3.
-- and complete the setup of the pipes.

select count(*) from customer_raw;

-- merge query is used for implementing scd-1 in the production tables:- 
merge into customer c 
using customer_raw cr
   on  c.customer_id = cr.customer_id
when matched and c.customer_id <> cr.customer_id or
                 c.first_name  <> cr.first_name  or
                 c.last_name   <> cr.last_name   or
                 c.email       <> cr.email       or
                 c.street      <> cr.street      or
                 c.city        <> cr.city        or
                 c.state       <> cr.state       or
                 c.country     <> cr.country then update
    set c.customer_id = cr.customer_id
        ,c.first_name  = cr.first_name 
        ,c.last_name   = cr.last_name  
        ,c.email       = cr.email      
        ,c.street      = cr.street     
        ,c.city        = cr.city       
        ,c.state       = cr.state      
        ,c.country     = cr.country  
        ,update_timestamp = current_timestamp()
when not matched then insert
           (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
    values (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);

    
select * from customer;


-- now here in the snowflake, the store procedure is created in the java language.
-- let's put the above query inside a store proceudre, so we don't have to run this big query and we have to just call the procedure.
-- the store procedure will frist run insert the updates in the customer table and truncate the raw tables.
-- so that the raw table does not have a large data and just the latest data only.

-- the last lines of the procudeure converts the text to sql script and then execute it.

merge into customer c 
using customer_raw cr
   on  c.customer_id = cr.customer_id
when matched and c.customer_id <> cr.customer_id or
                 c.first_name  <> cr.first_name  or
                 c.last_name   <> cr.last_name   or
                 c.email       <> cr.email       or
                 c.street      <> cr.street      or
                 c.city        <> cr.city        or
                 c.state       <> cr.state       or
                 c.country     <> cr.country then update
    set c.customer_id = cr.customer_id
        ,c.first_name  = cr.first_name 
        ,c.last_name   = cr.last_name  
        ,c.email       = cr.email      
        ,c.street      = cr.street     
        ,c.city        = cr.city       
        ,c.state       = cr.state      
        ,c.country     = cr.country  
        ,update_timestamp = current_timestamp()
when not matched then insert
           (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
    values (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);
    


CREATE OR REPLACE PROCEDURE pdr_scd_demo()
returns string not null
language javascript
as
    $$
      var cmd = `
                 merge into customer c 
                 using customer_raw cr
                    on  c.customer_id = cr.customer_id
                 when matched and c.customer_id <> cr.customer_id or
                                  c.first_name  <> cr.first_name  or
                                  c.last_name   <> cr.last_name   or
                                  c.email       <> cr.email       or
                                  c.street      <> cr.street      or
                                  c.city        <> cr.city        or
                                  c.state       <> cr.state       or
                                  c.country     <> cr.country then update
                     set c.customer_id = cr.customer_id
                         ,c.first_name  = cr.first_name 
                         ,c.last_name   = cr.last_name  
                         ,c.email       = cr.email      
                         ,c.street      = cr.street     
                         ,c.city        = cr.city       
                         ,c.state       = cr.state      
                         ,c.country     = cr.country  
                         ,update_timestamp = current_timestamp()
                 when not matched then insert
                            (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
                     values (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);
      `
      var cmd1 = "truncate table SCD_DEMO.SCD2.customer_raw;"
      var sql = snowflake.createStatement({sqlText: cmd});
      var sql1 = snowflake.createStatement({sqlText: cmd1});
      var result = sql.execute();
      var result1 = sql1.execute();
    return cmd+'\n'+cmd1;
    $$;

call pdr_scd_demo();
select * from customer;

select count(*) from customer;

use SCD_DEMO;

--now still we can see that all the process is still not fully-automatic.
-- not to make it fully-automatic, for this we will create tasks.
-- so basically tasks will run at certain interval of time.
-- now we need to understand that even if our data was real time, but here by the task it will be beocme as near-realtime.
-- and this is common , so for some talbles data will be in realtime while for other tables it will be near-realtime.

-- to create the tasks we need to give some permissions to groups and roles.

--Set up TASKADMIN role
use role securityadmin;
create or replace role taskadmin;

-- Set the active role to ACCOUNTADMIN before granting the EXECUTE TASK privilege to TASKADMIN
use role accountadmin;
grant execute task on account to role taskadmin;

-- Set the active role to SECURITYADMIN to show that this role can grant a role to another role 
use role securityadmin;
grant role taskadmin to role sysadmin;

-- run this tasks every one minutes.
create or replace task tsk_scd_raw warehouse = COMPUTE_WH schedule = '1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE=FALSE
as
call pdr_scd_demo();

show tasks;

--it's currently suspended, let's activate it.
alter task tsk_scd_raw resume;

alter task tsk_scd_raw suspend;--resume --suspend

show tasks;

select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state 
from table(information_schema.task_history()) where state = 'SCHEDULED' order by completed_time desc;


---------------------------------------------------------------------------------------------------------------------------






