from flask import Flask, render_template
from flask_socketio import SocketIO
import psycopg2
from datetime import datetime
from threading import Thread
import time

app = Flask(__name__)
socketio = SocketIO(app)

# Configuration de la base de données
# Configuration de la base de données PostgreSQL
db_params = {
    'dbname': 'DataBase_Polluants',
    'user': 'surelmanda',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

# Liste pour stocker les données
max_data_points = 10
predicted_data = {'time': [], 'actual_nox': [], 'predicted_nox': []}

# Fonction pour interroger la base de données
def fetch_data():
    while True:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = "SELECT cod_id, actual_nox, predicted_nox, read_timestamp FROM public.predicted_data ORDER BY read_timestamp DESC LIMIT 10;"

        cursor.execute(query)
        rows = cursor.fetchall()

        # Vérifier si des données sont disponibles
        if len(rows) > 0:
            # Mettre à jour les données
            predicted_data['time'] = [row[3].strftime('%Y-%m-%d %H:%M:%S') for row in reversed(rows)]
            predicted_data['actual_nox'] = [row[1] for row in reversed(rows)]
            predicted_data['predicted_nox'] = [row[2] for row in reversed(rows)]


            socketio.emit('update_plot', predicted_data)

        cursor.close()
        conn.close()

        time.sleep(2)

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('connect')
def handle_connect():
    print('Client connected')
    socketio.emit('update_plot', predicted_data)

if __name__ == '__main__':
    # Démarrer la tâche en arrière-plan pour interroger la base de données
    data_thread = Thread(target=fetch_data)
    data_thread.daemon = True
    data_thread.start()

    # Lancer l'application Flask avec SocketIO
    socketio.run(app, debug=True, port=5002)