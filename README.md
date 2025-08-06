## REQUIREMENTS
- Python 3.12
- Python library in requirements.txt
- Snowflake account
- Apache Airflow

## ENVIRONMENT SET UP
1.  Edit .env file inside python-script folder with your actual snowflake account
1.  Edit profiles.yml inside .dbt folder with your dbt authentication
1.  Build docker image
    ```shell
    docker build -f Dockerfile -t dbt:latest .
    ```
1.  Edit daily_temp_update.py Mount source for dbt_run and dbt_test. cd .dbt && pwd and then copy the directory into dag python script
    ```python
    mounts=[
        Mount(
            source=".dbt/", #edit this into your .dbt folder directory
            target="/root/.dbt",
            type="bind",
            )
        ],
    ```
1.  Move daily_temp_update.py into your airflow dags folder.
1.  Execute snowflake initial query (User, Role, Database creation) 
    ```shell
    python python-script/user_creation.py
    ```
1.  Execute snowflake import data query
    ```shell
    python python-script/data_import.py
    ```