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
    'Processing_data_spark',
    default_args=default_args,
    description='DAG for Spark job',
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
    bash_command='./create_Election_topics.sh',  # Remplacez par le chemin réel
    dag=dag,
)

# Tâche pour exécuter le script bash create_database_tables.sh
create_database_tables_task = BashOperator(
    task_id='create_database_and_tables',
    bash_command='./create_database_and_tables.sh',  # Remplacez par le chemin réel
    dag=dag,
)

# Tâche pour envoyer des données à Kafka
send_data_task = BashOperator(
    task_id='run_code_send_data_T1',
    bash_command='/usr/bin/python3 /Users/surelmanda/Downloads/AirGUARD/Pipeline-EcE/ExtractionData/send_data_to_kafka.py',
    dag=dag,
)

# Tâches Spark
spark_task_emploi = BashOperator(
    task_id='run_spark_job_Emploi',
    bash_command='/usr/bin/python3 /Users/surelmanda/Downloads/Ece-Pipelines/airflow/dags/JobSparkEmploi.py',
    dag=dag,
)

spark_task_logement = BashOperator(
    task_id='run_spark_job_Logement',
    bash_command='/usr/bin/python3 /Users/surelmanda/Downloads/Ece-Pipelines/airflow/dags/JobSparkLogement.py',
    dag=dag,
)

spark_task_population = BashOperator(
    task_id='run_spark_job_Population',
    bash_command='/usr/bin/python3 /Users/surelmanda/Downloads/Ece-Pipelines/airflow/dags/JobSparkPopulation.py',
    dag=dag,
)

spark_task_resultatsT1 = BashOperator(
    task_id='run_spark_job_resultatsT1',
    bash_command='/usr/bin/python3 /Users/surelmanda/Downloads/AirGUARD/Pipeline-EcE/JobsSpark/JobSparkResultatsT1.py',
    dag=dag,
)

insert_task_fact_votes = BashOperator(
    task_id='run_code_insert_votes',
    bash_command='/usr/bin/python3 /Users/surelmanda/Downloads/AirGUARD/Pipeline-EcE/dataModel/insert_fact_votes.py',
    dag=dag,
)

insert_task_departements = BashOperator(
    task_id='run_code_insert_depart',
    bash_command='/usr/bin/python3 /Users/surelmanda/Downloads/AirGUARD/Pipeline-EcE/dataModel/insert_dim_departement.py',
    dag=dag,
)

insert_task_candidats = BashOperator(
    task_id='run_code_insert_candidats',
    bash_command='/usr/bin/python3 /Users/surelmanda/Downloads/AirGUARD/Pipeline-EcE/dataModel/insert_dim_canidats.py',
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
start_processing_task >> create_topic_task >> create_database_tables_task
create_database_tables_task >> send_data_task
send_data_task >> spark_task_resultatsT1
spark_task_resultatsT1 >> [insert_task_candidats, insert_task_departements, insert_task_fact_votes] >> wait_five_second_task
# Ajoutez d'autres dépendances selon la logique de votre flux de travail
wait_five_second_task >> [spark_task_emploi, spark_task_logement, spark_task_population] >> end_processing_task