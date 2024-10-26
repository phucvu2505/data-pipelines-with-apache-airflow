import json
import pathlib

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

##
# The DAG class takes two required arguments
# dag_id
# start_date
# #
dag = DAG( # Instantiate a DAG object this is the staring point of any workflow
    dag_id="download_rocket_launches", # name of the DAG
    description="Download rocket pictures of recently launched rockets.",
    start_date=airflow.utils.dates.days_ago(14), # the date at which the DAG should first start running
    schedule_interval="@daily", # at what interval the DAG should run
)

download_launches = BashOperator( # Apply Bash to download the URL response with CURL
    task_id="download_launches", # name of task
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",  # noqa: E501
    dag=dag, # Reference to the DAG variable
)


def _get_pictures(): # Python function will parse the response and download all rocket pictures
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url) # get the image
                image_filename = image_url.split("/")[-1] # get only filename by selecting everything after last. Example: https://host/RockerImages/Electron.jpg_1440.jpg => Electron.jpg_1440.jpg
                target_file = f"/tmp/images/{image_filename}" # construct the target file path
                with open(target_file, "wb") as f: # Open target file path
                    f.write(response.content) # write image to file path
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")


get_pictures = PythonOperator( # Call the Python function in the DAG with a PythonOperator
    task_id="get_pictures", python_callable=_get_pictures, dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

download_launches >> get_pictures >> notify # set the order of execution of tasks
