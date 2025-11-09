-- Create environment
CREATE OR REPLACE WAREHOUSE demo_wh WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;
CREATE OR REPLACE DATABASE demo_db;
CREATE OR REPLACE SCHEMA demo_db.public;
USE DATABASE demo_db;
USE SCHEMA demo_db.public;

-- Target table
CREATE OR REPLACE TABLE emp_target (
  emp_id INT,
  emp_name STRING,
  dept STRING,
  salary NUMBER,
  last_updated TIMESTAMP_NTZ
);

-- Staging table
CREATE OR REPLACE TABLE emp_staging (
  emp_id INT,
  emp_name STRING,
  dept STRING,
  salary NUMBER,
  last_updated TIMESTAMP_NTZ
);

-- Stream on staging table
CREATE OR REPLACE STREAM emp_staging_stream ON TABLE emp_staging
  APPEND_ONLY = FALSE;

-- Stored procedure for merge
CREATE OR REPLACE PROCEDURE sp_merge_emp_changes()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  MERGE INTO emp_target t
  USING (SELECT * FROM emp_staging_stream) s
  ON t.emp_id = s.emp_id
  WHEN MATCHED THEN UPDATE SET
      t.emp_name = s.emp_name,
      t.dept = s.dept,
      t.salary = s.salary,
      t.last_updated = s.last_updated
  WHEN NOT MATCHED THEN
      INSERT (emp_id, emp_name, dept, salary, last_updated)
      VALUES (s.emp_id, s.emp_name, s.dept, s.salary, s.last_updated);
  RETURN 'Merge Complete';
END;
$$;

-- Task to automate merge every minute (demo frequency)
CREATE OR REPLACE TASK emp_task
  WAREHOUSE = demo_wh
  SCHEDULE = 'USING CRON * * * * * UTC'  -- every minute
AS
CALL sp_merge_emp_changes();

-- Start task
ALTER TASK emp_task RESUME;

