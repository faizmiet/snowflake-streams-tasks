# Snowflake Streams and Tasks Demo

This project demonstrates an incremental data pipeline in Snowflake using:
- **Streams** to track changes in staging
- **Tasks** to schedule automated merges into target tables
- **Stored procedure** for upsert (merge) logic

## Run (quick)
1. Create objects:
   snowsql -c <your-conn> -f sql/create_stream_task_demo.sql
2. Upload & load:
   snowsql -c <your-conn> -q "PUT file://./data/emp_delta.csv @%emp_staging AUTO_COMPRESS=FALSE;"
   snowsql -c <your-conn> -q "COPY INTO emp_staging FROM @%emp_staging FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1);"
3. Verify:
   snowsql -c <your-conn> -q "SELECT * FROM emp_target;"
