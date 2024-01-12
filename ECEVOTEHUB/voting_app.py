from flask import Flask, render_template, jsonify
import psycopg2

app = Flask(__name__)

# Configuration de la base de données
db_params = {
    'dbname': 'DB_Election_Ece',
    'user': 'surelmanda',
    'password': 'postgre',
    'host': 'localhost',
    'port': '5432',
}

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    # Requête SQL pour obtenir les données
    query = "SELECT candidate_name, photo_url, total_votes FROM resultat"
    cursor.execute(query)
    
    # Traitement des résultats de la requête
    data = [{'candidate_name': row[0], 'photo_url': row[1], 'total_votes': row[2]} for row in cursor.fetchall()]

    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)

# Fermeture de la connexion et du curseur lorsqu'on arrête l'application
@app.teardown_appcontext
def close_connection(exception=None):
    conn.close()
    cursor.close()
