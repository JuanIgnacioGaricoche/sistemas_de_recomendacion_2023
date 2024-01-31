# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 20:52:19 2024

@author: garic
"""

#%% Liberias

import os

import sqlite3
import pandas as pd
import numpy as np
import pickle

import lightfm as lfm
from lightfm import data
from lightfm import cross_validation
from lightfm import evaluation

from sklearn.model_selection import ParameterGrid, train_test_split

#%% Variables

selected_random_state = 42
THIS_FOLDER = os.path.dirname(os.path.abspath("__file__"))

#%% Divido datos en train y test

con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/alison_data.db"))
df_int = pd.read_sql_query("SELECT user_id, course_id, COUNT(*) AS cantidad_ratings, AVG(rating) AS avg_rating FROM interaction WHERE rating > 0 GROUP BY user_id, course_id", con)
df_items = pd.read_sql_query("SELECT * FROM course", con)


# X_train, X_test = train_test_split(df_int, test_size=0.2, random_state=selected_random_state)


#%%
# DefinO el espacio de búsqueda de hiperparámetros
param_grid = {
    'no_components': [1, 2, 3, 5, 8, 10, 20, 30],
    'learning_rate': [0.01, 0.05, 0.1, 0.12, 0.15],
    'k': [1, 2, 3, 5, 8, 10, 15],
    'learning_schedule': ['adagrad'],
    'n': [1,2,5,8,10,15,25],
    'item_alpha': [0.0],
    'user_alpha': [0.0],
    'loss': ['logistic'],
    'max_sampled': [1,2,5,8,10,12,15],
    # ... otros hiperparámetros
}

# Genero todas las combinaciones posibles
param_combinations = list(ParameterGrid(param_grid))

# Entrenar y evaluar el modelo para cada combinación
ds = lfm.data.Dataset()
ds.fit(users=df_int["user_id"].unique(), items=df_int["course_id"].unique())
user_id_map, user_feature_map, item_id_map, item_feature_map = ds.mapping()

(interactions, weights) = ds.build_interactions(df_int[["user_id", "course_id", "avg_rating"]].itertuples(index=False))

# X_train_interaction, X_test_interaction = train_test_split(interactions, test_size=0.2, random_state=selected_random_state)

results = {}

# raise Exception("Chequeo param_combinations")

def calcular_precision_modelo(params):
    print("Evaluo modelo", str(param_combinations.index(params)), "de ", len(param_combinations))
    model = lfm.LightFM(**params, random_state=selected_random_state)
    
    model.fit(interactions, sample_weight=weights, epochs=10)
    # predicciones = model.predict(user_id_map, item_id_map)
    precision = np.average(lfm.evaluation.precision_at_k(model=model, test_interactions=interactions, k=9))
    results[param_combinations.index(params)] = precision

for params in param_combinations:
    calcular_precision_modelo(params)




with open(os.path.join(THIS_FOLDER, "data/resultados_hiperparametros.pkl"), 'wb') as f:
    pickle.dump(results, f)

with open(os.path.join(THIS_FOLDER, "data/combinaciones_hiperparamentros.pkl"), 'wb') as f:
    pickle.dump(param_combinations, f)
    
# Segun results ordenado de mayor a menor precision
mejor_modelo = param_combinations[788]

