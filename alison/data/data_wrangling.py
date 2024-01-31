# -*- coding: utf-8 -*-
"""
Created on Wed Jan 10 20:37:46 2024

@author: garic
"""

#%% Libraries

import sys
import json

import pandas as pd
import sqlite3
from ast import literal_eval

from sqlalchemy import create_engine
engine = create_engine('sqlite:///C:/Users/garic/OneDrive/Escritorio/maestria_2023/repo/sistemas_recomendacion/scripts/alison/data/alison_data.db', echo=False)

#%% Data

all_interactions = pd.read_csv(r'C:\Users\garic\OneDrive\Escritorio\maestria_2023\repo\sistemas_recomendacion\scraping\raw_data\all_interactions.csv')
all_courses = pd.read_csv(r'C:\Users\garic\OneDrive\Escritorio\maestria_2023\repo\sistemas_recomendacion\scraping\raw_data\cursos_alison.csv')
all_profiles = pd.read_csv(r'C:\Users\garic\OneDrive\Escritorio\maestria_2023\repo\sistemas_recomendacion\scraping\raw_data\profiles_alison.csv')

#%% Processing

# Proximos pasos
# 1) Limpiar all_interactions para que tenga únicamente 3 columnas
# 2) Ver si se puede hacer coincidir las valoraciones de las interacciones de las 2 bbdd
# 3) Elegir las columnas con las que nos vamos a quedar en la tabla cursos y perfiles
# 4) Excluir los cursos y perfiles sin interacciones
# 5) Crear dataframe course_tag


all_interactions.groupby(by='rating', as_index=False).size()
all_interactions.groupby(by='dislikes_count', as_index=False).size()
all_interactions.groupby(by='likes_count', as_index=False).size()

# 1)
interactions = all_interactions[['user_id','course_id','created_at','rating']]
interactions['created_at'] = pd.to_datetime(interactions['created_at'])
interactions = interactions.groupby(by=['user_id','course_id','created_at'], as_index=False).aggregate({'rating':'min'})

# 2) No se puede. Voy a ver si es necesario
# 3) Ahora me quedo con todas las columnas. Despues en el script decido que columnas usar

# 4)
courses = all_courses[all_courses['id'].isin(all_interactions['course_id'].unique())]
courses = courses.rename(columns={'id':'course_id', 'course_type_id':'course_type'})

profiles = all_profiles[all_profiles['user_id'].isin(all_interactions['user_id'].unique())]

# 5) course_tag
courses_tags = courses[courses['tags'].notna()][['course_id','tags']]
courses_tags['tags'] = courses_tags['tags'].apply(literal_eval)
courses_tags = courses_tags[['course_id','tags']].explode('tags')

# 6) profile_tag
nested_profile_tag = profiles[profiles['skills']!='[]'][['user_id','skills']]
profile_tag = pd.DataFrame(columns = ['user_id','skills'])

for index, row in nested_profile_tag.iterrows():
    user_id = row['user_id']
    user_tags = literal_eval(row['skills'])
    for skill in user_tags:
        skill_name = skill['name']
        profile_tag.loc[len(profile_tag.index)] = [user_id, skill_name]
    
del index, row, user_id, user_tags, skill_name

profile_tag['skills'] = profile_tag['skills'].str.lower()
profile_tag['skills'] = profile_tag['skills'].str.replace(' ', '_')
profile_tag['skills'] = profile_tag['skills'].str.replace(',', '')

# Hacer un outer join para ver qué skills están en un lado pero no en el otro y ver si las puedo matchear
# skills_in_profiles = pd.DataFrame(profile_tag['skills'].unique(), columns = ['skills'])
# skills_in_courses = pd.DataFrame(courses_tags['tags'].unique(), columns = ['tags'])
# join = pd.merge(skills_in_profiles, skills_in_courses, how='inner', left_on='skills', right_on='tags')
# join = join[join['skills'].isna() | join['tags'].isna()]

#%% Database creation

cnx = sqlite3.connect(r'C:\Users\garic\OneDrive\Escritorio\maestria_2023\repo\sistemas_recomendacion\scripts\alison\data\alison_data.db')
c = cnx.cursor()

# # Interactions
# c.execute("""DROP TABLE IF EXISTS interaction;""")
# c.execute("""CREATE TABLE IF NOT EXISTS interaction(
#     id_interaction INTEGER PRIMARY KEY,
#     user_id INT,
#     course_id INT,
#     created_at TEXT,
#     rating INT,
#     UNIQUE (user_id, course_id, created_at)
#     );""")

# # Courses
# c.execute("""DROP TABLE IF EXISTS course;""")
# c.execute("""
#           CREATE TABLE IF NOT EXISTS course
#           (
#                 course_id INT PRIMARY KEY, 
#                 course_type INT, 
#                 publisher_name TEXT, 
#                 publisher_display_name TEXT, 
#                 publisher_slug TEXT, 
#                 active INT, 
#                 trending INT, 
#                 enrollable INT, 
#                 visibility INT, 
#                 responsive INT, 
#                 avg_duration TEXT, 
#                 list_ranking INT, 
#                 custom_list_ranking INT, 
#                 name TEXT, 
#                 slug TEXT, 
#                 headline TEXT, 
#                 outcomes TEXT, 
#                 locale TEXT, 
#                 rating INT, 
#                 enrolled INT, 
#                 certified INT, 
#                 category_name TEXT, 
#                 category_slug TEXT, 
#                 root_category_name TEXT, 
#                 root_category_slug TEXT, 
#                 level INT, 
#                 environment TEXT, 
#                 courseImgUrl TEXT, 
#                 points INT, 
#                 language TEXT, 
#                 published INT
#               )
#           """)

# # Profiles
# c.execute("""DROP TABLE IF EXISTS profile;""")
# c.execute("""CREATE TABLE IF NOT EXISTS profile
#           (user_id INT PRIMARY KEY,
#            firstname TEXT,
#            lastname TEXT,
#            birthdate TEXT,
#            sex INT,
#            percentage INT,
#            public INT,
#            picture_url TEXT,
#            job_title TEXT,
#            introduction TEXT,
#            contact_number TEXT,
#            email TEXT,
#            visible_course_ids TEXT,
#            show_goals INT,
#            show_psychometric INT,
#            show_hobbies INT,
#            show_books INT,
#            show_movies INT,
#            show_sports INT,
#            educations TEXT,
#            languages TEXT,
#            professional_experiences TEXT,
#            psychometric_quiz_results TEXT,
#            additional_certifications TEXT,
#            development_goals TEXT,
#            interests TEXT,
#            english_test_visible INT,
#            recommendations TEXT,
#            cvs TEXT,
#            skills TEXT,
#            profile_cover_image TEXT,
#            country TEXT,
#            state TEXT,
#            city TEXT,
#            allow_connections INT
# );"""
#           )

# # Course_tag
# c.execute("""DROP TABLE IF EXISTS course_tag;""")
# c.execute("""CREATE TABLE IF NOT EXISTS course_tag
#           (course_tag_id INTEGER PRIMARY KEY,
#            course_id INT,
#            tags TEXT
# );"""
#           )

# # Profile_tag
# c.execute("""DROP TABLE IF EXISTS profile_tag;""")
# c.execute("""CREATE TABLE IF NOT EXISTS profile_tag
#           (profile_tag_id INTEGER PRIMARY KEY,
#            user_id INT,
#            skills TEXT
# );"""
#           )

#%% Data insertion

# Interactions
interactions.to_sql('interaction', con=engine, if_exists='replace', index=False)
courses[['course_id', 'course_type', 'publisher_name', 'publisher_display_name', 'publisher_slug', 'active', 'trending', 'enrollable', 'visibility', 'responsive', 'avg_duration', 'list_ranking', 'custom_list_ranking', 'name', 'slug', 'headline', 'outcomes', 'locale', 'rating', 'enrolled', 'certified', 'category_name', 'category_slug', 'root_category_name', 'root_category_slug', 'level', 'environment', 'courseImgUrl', 'points', 'language', 'published']].to_sql('course', con=engine, if_exists='replace', index=False)
profiles.to_sql('profile', con=engine, if_exists='replace', index=False)
courses_tags.to_sql('course_tag', con=engine, if_exists='replace', index=False)
profile_tag.to_sql('profile_tag', con=engine, if_exists='replace', index=False)
print("Created and inserted all tables")

#%% Need to set up firstname,lastname as Unique Key
# For this, I delete all duplicated registers by running this query

c.execute("""DELETE FROM profile WHERE user_id IN
(
WITH a AS
(
SELECT ROW_NUMBER(*) OVER(PARTITION BY firstname, lastname) AS repeated, *
FROM profile
)
SELECT user_id
FROM a
WHERE repeated > 1
)""")

#%% Closing connection

cnx.close()
print("Connection closed")

sys.exit()