# Snowflake Streams and Tasks Demo

Demonstrates an incremental data pipeline using:
- **Streams** to detect changes in staging
- **Tasks** to automate merge into target table
- **Stored procedure** for upsert logic (simulated CDC)

### Steps
1. Run `sql/create_stream_task_demo.sql` to create tables, stream, task, and procedure.
2. Load sample data:
   ```bash
   snowsql -q "PUT file://./data/emp_delta.csv @%emp_staging AUTO_COMPRESS=FALSE;"
   snowsql -q "COPY INTO emp_staging FROM @%emp_staging FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1);"

