from flask import Flask, render_template, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

app = Flask(__name__)

# Remplacez les informations ci-dessous par celles de votre base de données
db_url = "postgresql://surelmanda:postgre@localhost:5432/DB_Election_Ece"
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
session = Session()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    # Use an appropriate SQL query that includes the vote count
    query = text("SELECT candidate_name, photo_url, total_votes FROM resultat")
    result = session.execute(query)

    # Process the query result
    data = [{'candidate_name': row[0], 'photo_url': row[1], 'total_votes': row[2]} for row in result]

    return jsonify(data)
if __name__ == '__main__':
    app.run(debug=True)
