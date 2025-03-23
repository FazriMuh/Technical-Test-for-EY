# Technical Test EY

## Installation

### Setting Up Python Environment

Before running the project, you need to set up a Python environment and install the required packages.

```bash
# Create a virtual environment
python -m venv ey_env

# Activate the virtual environment
# For Unix/Mac
source ey_env/bin/activate  

# For Windows
ey_env\Scripts\activate  
```

### Package Installation

Install the necessary packages using the following command:

```bash
pip install pyspark prefect
```

## How to Set Up

To start the Prefect server, run the following command in your terminal:

```bash
prefect server start
```

This will launch a temporary Prefect server, allowing you to monitor and manage your workflows.

Open a new terminal and execute the following command to manually run the ETL pipeline:

```bash
python3 etl_pipeline.py
```

The above command is a manual execution test for the ETL pipeline.

## Deploy & Schedule

To deploy and schedule the ETL pipeline, run the following command:

```bash
prefect deploy etl_pipeline.py:etl_pipeline -n EY_etl_deployment
```

During the deployment, you will be prompted to select a work pool. Choose the appropriate work pool and configure the schedule using a cron expression.

For example, setting the cron schedule to `*/2 * * * *` will run the pipeline every 2 minutes. Select `Asia/Jakarta` as the timezone and activate the schedule.

After deployment, execute the following command to set the Prefect API URL:

```bash
export PREFECT_API_URL="http://127.0.0.1:4200/api"
```

Then, start the Prefect worker:

```bash
prefect worker start -p "ey_etl_process"
```

## Project Structure

Ensure the folder structure looks like this:

```
/TechTest-EY
│── /env                     
│── etl_pipeline.py          
│── postgresql-42.7.4.jar    
│── sample_data.json         

```

## Running the ETL Pipeline

To manually trigger a scheduled ETL pipeline execution, use the following command:

```bash
prefect deployment run 'etl-pipeline/EY_etl_deployment'
```

This ensures that the pipeline runs as per the configured schedule while also allowing for manual execution when needed.

## Additional Information

For more details on running a dedicated Prefect server, refer to the official documentation:

[Prefect Self-Hosting Guide](https://docs.prefect.io/3.0/manage/self-host#self-host-a-prefect-server)

