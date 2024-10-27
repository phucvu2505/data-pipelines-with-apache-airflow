import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="listing_4_01",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly",
)

get_data = BashOperator(
    task_id="get_data",
    bash_command=(
        "curl -o /tmp/wikipageviews.gz "
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year }}/"
        "{{ execution_date.year }}-{{ '{:02}'.format(execution_date.month) }}/"
        "pageviews-{{ execution_date.year }}"
        "{{ '{:02}'.format(execution_date.month) }}" # The Wikipedia pageviews URL required zero-padded months, days, and hour (e.g: "07" for hour 7). within the Jinja-template string ww therefore apply string format for padding '{:02}'.format(execution_date.hour) 
        "{{ '{:02}'.format(execution_date.day) }}-"
        "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
    ),
    dag=dag,
)

# #
# the double curly braces tell Junja there's a variable or expression inside to evaluate
# the value of execution_date is not know when progaming beacause the user will their execution_date in the form runtime
# Airflow uses the Pendulum library for datetimes => execution_date is such as Pendulum datetime object
# Trong Apache Airflow, Jinja templating được sử dụng để tạo ra các chuỗi động trong các tác vụ (tasks). 
# Nó cho phép bạn chèn các giá trị động vào trong các tham số của DAG (Directed Acyclic Graph) hoặc task. 
# Jinja giúp bạn làm cho các tác vụ linh hoạt hơn bằng cách sử dụng các biến, hàm và điều kiện.
#