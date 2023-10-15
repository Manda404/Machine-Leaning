# Gas Turbine Emissions

# Modélisation Prédictive des Émissions des Turbines à Gaz

## Problématique

Les turbines à gaz sont largement utilisées dans le monde entier pour la production d'électricité. Avec la sensibilisation croissante aux gaz à effet de serre, des réglementations strictes limitent les émissions des turbines à gaz. Ces émissions sont généralement mesurées par des capteurs ; cependant, les inexactitudes des capteurs dues à des dysfonctionnements ou des problèmes d'étalonnage peuvent entraîner des données erronées.

Les turbines à gaz dans les centrales électriques fonctionnent à des tours par minute constantes, mais les niveaux d'émission fluctuent en fonction de divers facteurs tels que la température, la pression atmosphérique et l'humidité. Ces variables varient tout au long de la journée et des mois, nécessitant une mesure précise des niveaux d'émission. La création d'un modèle d'apprentissage automatique capable de prédire avec précision les niveaux d'émission pourrait servir de système complémentaire aux dispositifs de surveillance par capteurs. En cas de dysfonctionnement du capteur, le modèle d'apprentissage automatique peut détecter la fonctionnalité du capteur et agir comme un système d'alerte.

## Objectif

L'objectif de ce projet est de développer un modèle d'apprentissage automatique capable de prédire avec précision les niveaux d'émission des turbines à gaz, en se concentrant particulièrement sur le Monoxyde de Carbone (CO) et les Oxydes d'Azote (NOX).

## Présentation du Jeu de Données

Le jeu de données comprend 36 733 lignes et 11 colonnes, comprenant 9 caractéristiques numériques et deux variables cibles (CO et NOX). L'article de recherche, disponible sur [ce lien](https://journals.tubitak.gov.tr/elektrik/issues/elk-19-27-6/elk-27-6-54-1807-87.pdf), discute de la conception d'un modèle d'apprentissage automatique avec une Erreur Absolue Moyenne (MAE) exceptionnellement faible.

## Importance

En cas de succès, ce projet peut considérablement améliorer les systèmes de surveillance des turbines à gaz, garantissant un suivi précis des émissions, même en l'absence de capteurs fonctionnels. De plus, ce modèle prédictif pourrait servir de système d'alerte précoce, avertissant les opérateurs des dysfonctionnements des capteurs et garantissant une maintenance opportune.

*Remarque : Le jeu de données et les articles de recherche ont été collectés et publiés par les auteurs mentionnés dans la source fournie.*
