from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'GroupPipeline',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 28, 10, 00),
    'end_date': datetime(2023, 12, 31, 10, 00),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'Data_Processing_Election',
    default_args=default_args,
    description='DAG for data processing Election des délégués',
    schedule_interval=timedelta(days=1),
)

# Tâche initiale sans commande réelle, utilisée comme point de départ
start_processing_task = BashOperator(
    task_id='start_processing',
    bash_command='echo "Start ETL (extract, transform, load) processing"',
    dag=dag,
)

# Tâche pour exécuter le script bash create_topic.sh
create_topic_task = BashOperator(
    task_id='create_topic',
    bash_command='./manage_topics_data.sh',  # Remplacez par le chemin réel
    dag=dag,
)

# Tâche pour exécuter le script bash create_database_tables.sh
create_database_tables_task = BashOperator(
    task_id='create_database_and_tables',
    bash_command='./create_database_and_tables.sh',  # Remplacez par le chemin réel
    dag=dag,
)

# Tâche pour envoyer des données à Kafka
run_task_Inscriptions = BashOperator(
    task_id='run_code_Inscriptions',
    bash_command='/usr/bin/python3 /Users/surelmanda/Downloads/AirGUARD/ECEVOTEHUB_Analyse_Election_Real_Time/Code_Python/Inscriptions.py',
    dag=dag,
)

# Tâches Spark
run_task_Elections = BashOperator(
    task_id='run_code_Elections',
    bash_command='/usr/bin/python3 /Users/surelmanda/Downloads/AirGUARD/ECEVOTEHUB_Analyse_Election_Real_Time/Code_Python/Elections.py',
    dag=dag,
)

spark_task_election = BashOperator(
    task_id='run_spark_job_election',
    bash_command='/usr/bin/python3 /Users/surelmanda/Downloads/AirGUARD/ECEVOTEHUB_Analyse_Election_Real_Time/JobSpark/JobSparkStreamSaveToTable.py',
    dag=dag,
)

# Tâche finale sans commande réelle, utilisée comme point d'arrivée
end_processing_task = BashOperator(
    task_id='end_processing',
    bash_command='echo "End processing"',
    dag=dag,
)

wait_five_second_task = BashOperator(
    task_id='sleep_five_second',
    bash_command='sleep 5',
    dag=dag,
)

# Définition des dépendances entre les tâches
start_processing_task >> create_topic_task >> create_database_tables_task >> run_task_Inscriptions >> run_task_Elections >> wait_five_second_task
wait_five_second_task >> end_processing_task