import sqlite3
import pandas as pd
import sys
import os

import lightfm as lfm
from lightfm import data
from lightfm import cross_validation
from lightfm import evaluation
import surprise as sp

import whoosh as wh
from whoosh import fields
from whoosh import index
from whoosh import qparser

THIS_FOLDER = os.path.dirname(os.path.abspath("__file__"))

def sql_execute_fetchone(query):
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/alison_data.db"))
    cur = con.cursor()
    res = cur.execute(query)
    result = res.fetchone()[0]
    return result

def sql_execute(query, params=None):
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/alison_data.db"))
    cur = con.cursor()    
    if params:
        res = cur.execute(query, params)
    else:
        res = cur.execute(query)
    
    con.commit()
    con.close()
    return

def sql_select(query, params=None):
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/alison_data.db"))
    con.row_factory = sqlite3.Row # esto es para que devuelva registros en el fetchall
    cur = con.cursor()    
    if params:
        res = cur.execute(query, params)
    else:
        res = cur.execute(query)
    
    ret = res.fetchall()
    con.close()

    return ret

def crear_usuario(lastname):
    # Aca hice una chanchada. Inserto un vacio en firstname, para asegurarme de que no sea nulo.
    # Asi me aseguro que no se repitan usuarios con el mismo firstname,lastname ya que en la bbdd son unique key.
    query = "INSERT INTO profile(user_id, firstname,lastname) VALUES ((SELECT MAX(user_id)+1 FROM profile),'', ?) ON CONFLICT DO NOTHING;" # si firstname,lastname existia, se produce un conflicto y le digo que no haga nada
    sql_execute(query, (lastname,))
    return

def insertar_interacciones(course_id, user_id, rating, interacciones="interaction"):
    # Voy a estar insertando un registro por cada interaccion. Saco el on conflict, ya que le agregue timestamp y no tengo restriccion de pk o uk.
    query = f"INSERT INTO {interacciones}(course_id, user_id, created_at, rating) VALUES (?, ?, CURRENT_TIMESTAMP, ?);"# ON CONFLICT (course_id, user_id) DO UPDATE SET rating=?, created_at=CURRENT_TIMESTAMP;" # si el rating existia lo actualizo
    sql_execute(query, (course_id, user_id, rating))
    return

def reset_usuario(user_id, interacciones="interaction"):
    query = f"DELETE FROM {interacciones} WHERE user_id = ?;"
    sql_execute(query, (user_id,))
    return

def obtener_libro(id_libro):
    query = "SELECT * FROM libros WHERE id_libro = ?;"
    libro = sql_select(query, (id_libro,))[0]
    return libro

def valorados(user_id):
    # Se puede tener mas de una interaccion por usuario e item. Por eso agrupo
    query = "SELECT user_id, course_id FROM interaction WHERE user_id = ? AND rating > 0 GROUP BY user_id, course_id"
    valorados = sql_select(query, (user_id,))
    return valorados

def ignorados(user_id):
    # Se puede tener mas de una interaccion por usuario e item. Por eso agrupo
    # Excluyo los cursos sobre los que alguna vez se realizo una valoracion != 0
    query = """SELECT user_id, course_id
            FROM interaction
            WHERE user_id = ?
            	AND rating = 0
            	AND course_id NOT IN (SELECT DISTINCT course_id FROM interaction WHERE user_id = ? AND rating>0)
            GROUP BY user_id, course_id"""
    ignorados = sql_select(query, (user_id,user_id))
    return ignorados

def datos_cursos(id_cursos):
    query = f"SELECT DISTINCT * FROM course WHERE course_id IN ({','.join(['?']*len(id_cursos))})"
    cursos = sql_select(query, id_cursos)
    return cursos

# def recomendar_top_9(id_lector, interacciones="interacciones"):
#     query = f"""
#         SELECT id_libro, AVG(rating) as rating, count(*) AS cant
#           FROM {interacciones}
#          WHERE id_libro NOT IN (SELECT id_libro FROM {interacciones} WHERE id_lector = ?)
#            AND rating > 0
#          GROUP BY 1
#          ORDER BY 3 DESC, 2 DESC
#          LIMIT 9
#     """
#     id_libros = [r["id_libro"] for r in sql_select(query, (id_lector,))]
#     return id_libros

def recomendar_mas_populares(user_id, interacciones="interacciones"):
 #Recomienda los 9 mas populares con los que el usuario no interactuo
    query = f"""
            WITH most_populars AS
            (
            SELECT
            course_id,
            AVG(rating) AS rating_promedio,
            COUNT(*) AS cantidad_valoraciones,
            (AVG(rating)*COUNT(*))/(COUNT(*)+1) AS rating_ponderado
            FROM interaction
            WHERE 1=1
            AND rating > 0
            GROUP BY course_id
            )
            SELECT
            	*
            FROM most_populars
            WHERE most_populars.course_id NOT IN (SELECT DISTINCT course_id FROM interaction WHERE user_id = {user_id} AND rating>0)
            ORDER BY 4 DESC
            LIMIT 9
    """
    cursos = sql_select(query)
    id_cursos = list([curso[0] for curso in cursos])
    return id_cursos

def listar_items_por_popularidad(interacciones="interaction"):
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/alison_data.db"))
    # Para desempatar en las recomendaciones basadas en el perfil, uso el rating_ponderado
    query = f"""
            SELECT
            course_id,
            AVG(rating) AS rating_promedio,
            COUNT(*) AS cantidad_valoraciones,
            (AVG(rating)*COUNT(*))/(COUNT(*)+1) AS rating_ponderado
            FROM {interacciones}
            WHERE 1=1
            AND rating > 0
            GROUP BY course_id
            ORDER BY 4 DESC
            """
    cursos = pd.read_sql_query(query, con)
    con.close()
    #id_cursos = list([curso[0] for curso in cursos])
    return cursos

def consultar_user_id(lastname):
    query = "SELECT user_id FROM profile WHERE lastname = {}".format(repr(lastname))
    user_id = sql_execute_fetchone(query)
    return user_id

def recomendar_perfil(user_id):
    # El recomendador se basa en las interacciones del usuario
    # Recomienda cursos similares (en categoria y ambiente) a los cursos con los que interactuo
    # Para desempatar, usa el rating ponderado. Ese rating pondera los scores en base a la cantidad de valoraciones del curso
    # TODO: usar datos del usuario para el perfil

    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/alison_data.db"))
    # Me traigo las interacciones != 0 del usuario
    df_int = pd.read_sql_query(f"SELECT user_id, course_id, COUNT(*) AS cantidad_ratings, AVG(rating) AS avg_rating FROM interaction WHERE user_id = {user_id} AND rating > 0 GROUP BY user_id, course_id", con)
    df_items = pd.read_sql_query("SELECT * FROM course", con)
    df_usuarios = pd.read_sql_query("SELECT * FROM profile", con)
    con.close()
    
    perf_items = pd.get_dummies(df_items[["course_id", "category_slug", "environment"]], columns=["category_slug", "environment"]).set_index("course_id")
    perf_usuario = pd.merge(df_int, perf_items, how='inner', on='course_id')

    for c in perf_usuario.columns:
        if c.startswith("category_slug_") | c.startswith("environment_"):
            perf_usuario[c] = perf_usuario[c] * perf_usuario["avg_rating"]

    perf_usuario = perf_usuario.drop(columns=["course_id", "avg_rating", "cantidad_ratings"]).groupby("user_id").mean()
    #perf_usuario = perf_usuario / perf_usuario.sum(axis=1)[0] # normalizo. No funciono. Uso linea de abajo
    perf_usuario = perf_usuario.div(perf_usuario.sum(axis=1), axis=0)
    
    for g in perf_items.columns:
        #perf_items[g] = perf_items[g] * perf_usuario[g]
        perf_items[g] = perf_items[g].apply(lambda x: x*perf_usuario[g])


    cursos_valorados = df_int['course_id'].tolist()
    cursos_segun_popularidad = listar_items_por_popularidad(interacciones="interaction")
    cursos_segun_perfil = perf_items.sum(axis=1).sort_values(ascending=False).reset_index().rename(columns={0:'profile_based_score'})
    cursos = pd.merge(cursos_segun_perfil, cursos_segun_popularidad, how='left', on='course_id')
    
    recomendaciones = cursos[~cursos['course_id'].isin(cursos_valorados)].sort_values(by=['profile_based_score','rating_ponderado'], ascending=[False, False])['course_id'][:9].tolist()
    #recomendaciones = [l for l in perf_items.sum(axis=1).sort_values(ascending=False).index if l not in cursos_valorados][:9]
    
    return recomendaciones

def recomendar_lightfm(user_id, interacciones="interaction"):

    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/alison_data.db"))
    df_int = pd.read_sql_query(f"SELECT * FROM {interacciones} WHERE rating > 0", con)
    df_int = pd.read_sql_query(f"SELECT user_id, course_id, COUNT(*) AS cantidad_ratings, AVG(rating) AS avg_rating FROM {interacciones} WHERE rating > 0 GROUP BY user_id, course_id", con)
    df_items = pd.read_sql_query("SELECT * FROM course", con)
    con.close()

    ds = lfm.data.Dataset()
    ds.fit(users=df_int["user_id"].unique(), items=df_items["course_id"].unique())
    
    user_id_map, user_feature_map, item_id_map, item_feature_map = ds.mapping()
    (interactions, weights) = ds.build_interactions(df_int[["user_id", "course_id", "avg_rating"]].itertuples(index=False))

    # Los hiperparamentros se definieron segun el script hyper_tuning.py
    model = lfm.LightFM(no_components=8, k=1, n=1, learning_schedule='adagrad', loss='logistic', learning_rate=0.1, item_alpha=0.0, user_alpha=0.0, max_sampled=1, random_state=42)
    model.fit(interactions, sample_weight=weights, epochs=10)

    cursos_tomados = df_int[df_int['user_id']==int(user_id)]['course_id'].tolist()
    todos_los_cursos = df_items["course_id"].tolist()
    cursos_no_tomados = set(todos_los_cursos).difference(cursos_tomados)
    user_id_for_prediction = int(user_id)
    predicciones = model.predict(user_id_map[user_id_for_prediction], [item_id_map[l] for l in cursos_no_tomados])

    recomendaciones = sorted([(p, l) for (p, l) in zip(predicciones, cursos_no_tomados)], reverse=True)[:9]
    recomendaciones = [curso[1] for curso in recomendaciones]
    # raise Exception("Chequear por que esta recomendando cursos ya tomados")
    return recomendaciones

def recomendar_surprise(id_lector, interacciones="interacciones"):
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/data.db"))
    df_int = pd.read_sql_query(f"SELECT * FROM {interacciones}", con)
    df_items = pd.read_sql_query("SELECT * FROM libros", con)
    con.close()
    
    reader = sp.reader.Reader(rating_scale=(1, 10))

    data = sp.dataset.Dataset.load_from_df(df_int.loc[df_int["rating"] > 0, ['id_lector', 'id_libro', 'rating']], reader)
    trainset = data.build_full_trainset()
    model = sp.prediction_algorithms.matrix_factorization.SVD(n_factors=500, n_epochs=20, random_state=42)
    model.fit(trainset)

    libros_leidos_o_vistos = df_int.loc[df_int["id_lector"] == id_lector, "id_libro"].tolist()
    todos_los_libros = df_items["id_libro"].tolist()
    libros_no_leidos_ni_vistos = set(todos_los_libros).difference(libros_leidos_o_vistos)
    
    predicciones = [model.predict(id_lector, l).est for l in libros_no_leidos_ni_vistos]
    recomendaciones = sorted([(p, l) for (p, l) in zip(predicciones, libros_no_leidos_ni_vistos)], reverse=True)[:9]

    recomendaciones = [libro[1] for libro in recomendaciones]
    return recomendaciones

def recomendar_whoosh(id_lector, interacciones="interacciones"):
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/data.db"))
    df_int = pd.read_sql_query(f"SELECT * FROM {interacciones}", con)
    df_items = pd.read_sql_query("SELECT * FROM libros", con)
    con.close()

    # TODO: usar cant
    terminos = []    
    for campo in ["editorial", "autor", "genero"]:
        query = f"""
            SELECT {campo} AS valor, count(*) AS cant
            FROM interacciones AS i JOIN libros AS l ON i.id_libro = l.id_libro
            WHERE id_lector = ?
            AND rating > 0
            GROUP BY {campo}
            HAVING cant > 1
            ORDER BY cant DESC
            LIMIT 3
        """       
        rows = sql_select(query, (id_lector,))

        for row in rows:
            terminos.append(wh.query.Term(campo, row["valor"]))
    
    query = wh.query.Or(terminos)

    libros_leidos_o_vistos = df_int.loc[df_int["id_lector"] == id_lector, "id_libro"].tolist()

    # TODO: usar el scoring
    # TODO: ampliar la busqueda con autores parecidos (matriz de similitudes de autores)
    ix = wh.index.open_dir("indexdir")
    with ix.searcher() as searcher:
        results = searcher.search(query, terms=True, scored=True, limit=1000)
        recomendaciones = [r["id_libro"] for r in results if r not in libros_leidos_o_vistos][:9]

    return recomendaciones

def recomendar(user_id, interacciones="interaction"):
    # TODO: combinar mejor los recomendadores
    # TODO: crear usuarios fans para llenar la matriz
    cant_valorados = len(valorados(user_id))

    if cant_valorados <= 5:
        print("recomendador: 9maspopulares", file=sys.stdout)
        id_cursos = recomendar_mas_populares(user_id, interacciones)
    elif cant_valorados <= 10:
        print("recomendador: perfil", file=sys.stdout)
        id_cursos = recomendar_perfil(user_id)
        # id_cursos = recomendar_lightfm(user_id, interacciones)
    else:
        print("recomendador: lightfm", file=sys.stdout)
        #id_cursos = recomendar_surprise(id_lector, interacciones)
        id_cursos = recomendar_lightfm(user_id, interacciones)
        #id_cursos = recomendar_whoosh(user_id, interacciones)
        # id_cursos = recomendar_mas_populares(user_id, interacciones)


    # TODO: como completo las recomendaciones cuando vienen menos de 9?
    recomendaciones = datos_cursos(id_cursos)

    return recomendaciones
