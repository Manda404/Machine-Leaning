from flask import Flask, jsonify
import random

app = Flask(__name__)

@app.route('/polluants', methods=['GET'])
def get_pollutant_data():
    proba_nulle = 0.15  # Ajustez la probabilité d'avoir des valeurs nulles selon vos besoins

    # Fonction pour générer une valeur aléatoire entre 0 et 10 ou None (null) ou nan valeurs manquantes
    def generate_random_value():
        return None if random.random() < proba_nulle else random.uniform(0, 10)

    # Génère des valeurs aléatoires pour SO2, NO2, O3, PM10

    pollutants_data = {
        'so2': generate_random_value(),
        'no2': generate_random_value(),
        'o3': generate_random_value(),
        'pm10': generate_random_value(),
    }

    return jsonify(pollutants_data)

if __name__ == '__main__':
    app.run(debug=True)
