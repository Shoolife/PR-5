import os
import json
import pandas as pd
from pymongo import MongoClient
from bson import json_util

client = MongoClient('mongodb://localhost:27017/')
db = client['cinema_database']
movies_collection = db['movies']
actors_collection = db['actors']

result_dir = 'result'
os.makedirs(result_dir, exist_ok=True)

with open('movies.json', 'r', encoding='utf-8') as file:
    movies_data = json.load(file)
movies_collection.insert_many(movies_data)

actors_data = pd.read_csv('actors.csv').to_dict(orient='records')
actors_collection.insert_many(actors_data)

def save_query_result(filename, query):
    with open(os.path.join(result_dir, filename), 'w', encoding='utf-8') as file:
        file.write(json_util.dumps(query, indent=4, ensure_ascii=False))

# Категория 1: Выборка 
query = list(movies_collection.find({"genre": "Sci-Fi"}))
save_query_result('movies_sci_fi.json', query)

query = list(movies_collection.find({"rating": {"$gt": 8.5}}))
save_query_result('movies_high_rating.json', query)

query = list(actors_collection.find({"$expr": {"$gt": [{"$size": {"$split": ["$movies", ";"]}}, 5]}}))
save_query_result('actors_more_than_5_movies.json', query)

query = list(actors_collection.find({"age": {"$gt": 60}}))
save_query_result('actors_above_60.json', query)

query = list(movies_collection.find({"year": {"$gt": 2010}}))
save_query_result('movies_after_2010.json', query)

# Категория 2: Выборка с агрегацией
query = list(movies_collection.aggregate([
    {"$group": {"_id": "$genre", "count": {"$sum": 1}}}
]))
save_query_result('movies_genre_count.json', query)

query = list(actors_collection.aggregate([
    {"$match": {"movies": {"$regex": ".*Drama.*"}}},
    {"$group": {"_id": None, "avg_age": {"$avg": "$age"}}}
]))
save_query_result('actors_avg_age_drama.json', query)

query = list(movies_collection.aggregate([
    {"$group": {"_id": "$director", "avg_rating": {"$avg": "$rating"}}}
]))
save_query_result('movies_avg_rating_director.json', query)

query = list(actors_collection.aggregate([
    {"$project": {"name": 1, "movies_count": {"$size": {"$split": ["$movies", ";"]}}}}
]))
save_query_result('actors_movies_count.json', query)

query = list(actors_collection.aggregate([
    {"$match": {"movies": {"$regex": ".*"}}}, 
    {"$group": {"_id": None, "max_age": {"$max": "$age"}}}
]))
save_query_result('actors_max_age_movies_after_2000.json', query)

# Категория 3: Обновление/Удаление данных
actors_collection.update_many({}, {"$inc": {"age": 1}})
updated_actors = list(actors_collection.find())
with open(os.path.join(result_dir, 'after_age_increment.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(updated_actors, indent=4, ensure_ascii=False))

movies_collection.update_many({"rating": {"$lt": 8}}, {"$inc": {"rating": 0.5}})
updated_movies = list(movies_collection.find())
with open(os.path.join(result_dir, 'after_rating_increment.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(updated_movies, indent=4, ensure_ascii=False))

movies_collection.delete_many({"year": {"$lt": 1980}})
remaining_movies = list(movies_collection.find())
with open(os.path.join(result_dir, 'after_old_movies_deletion.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(remaining_movies, indent=4, ensure_ascii=False))

actors_collection.delete_many({"movies": {"$eq": ""}})
remaining_actors = list(actors_collection.find())
with open(os.path.join(result_dir, 'after_empty_actors_deletion.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(remaining_actors, indent=4, ensure_ascii=False))

actors_collection.update_many({"movies": {"$regex": ".*Sci-Fi.*"}}, {"$mul": {"age": 1.1}})
updated_sci_fi_actors = list(actors_collection.find())
with open(os.path.join(result_dir, 'after_sci_fi_actors_update.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(updated_sci_fi_actors, indent=4, ensure_ascii=False))
