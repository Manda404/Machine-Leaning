import pandas as pd
from sklearn.model_selection import train_test_split

# Charger le dataset
dataset_path = "/Users/surelmanda/Downloads/AirGUARD/Gas_Turbine_Streaming/gas_turbine_full.csv"
df = pd.read_csv(dataset_path)

# Diviser le dataset en train (80%) et test (20%) avec shuffle activé
train_df, test_df = train_test_split(df, test_size=0.2, random_state=42, shuffle=True)

# Afficher les dimensions de chaque ensemble de données
print("Dimensions de data_train : ", train_df.shape)
print("Dimensions de data_test : ", test_df.shape)

# Chemin vers le répertoire de sauvegarde
save_path = "/Users/surelmanda/Downloads/AirGUARD/Gas_Turbine_Streaming/data/"

# Enregistrer les datasets dans des fichiers CSV
train_df.to_csv(save_path + "data_train.csv", index=False)
test_df.to_csv(save_path + "data_test.csv", index=False)
