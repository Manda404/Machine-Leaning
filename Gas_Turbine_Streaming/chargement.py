from joblib import load

# Chemin vers votre modèle
model_path = "/Users/surelmanda/Downloads/AirGUARD/Gas_Turbine_Streaming/Models/modele_entrene.joblib"

# Charger le modèle
loaded_model = load(model_path)

# Vérifier la version de Scikit-learn utilisée lors de l'enregistrement
if hasattr(loaded_model, 'sklearn_version'):
    print(f"Le modèle a été enregistré avec Scikit-learn version {loaded_model.sklearn_version}.")
else:
    print("Les informations sur la version de Scikit-learn ne sont pas disponibles.")