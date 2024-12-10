import os
import pandas as pd
from pymongo import MongoClient
from bson import json_util

client = MongoClient('mongodb://localhost:27017/')
db = client['task_database']
collection = db['task_collection']

file_path = 'task_2_item.pkl'
data_task_2 = pd.read_pickle(file_path)  
collection.insert_many(data_task_2)  

result_dir = 'result'
os.makedirs(result_dir, exist_ok=True)


salary_stats = collection.aggregate([
    {"$group": {
        "_id": None,
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }}
])
with open(os.path.join(result_dir, 'salary_stats.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(list(salary_stats), indent=4, ensure_ascii=False))

job_count = collection.aggregate([
    {"$group": {
        "_id": "$job",
        "count": {"$sum": 1}
    }},
    {"$sort": {"count": -1}}
])
with open(os.path.join(result_dir, 'job_count.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(list(job_count), indent=4, ensure_ascii=False))

salary_by_city = collection.aggregate([
    {"$group": {
        "_id": "$city",
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }},
    {"$sort": {"_id": 1}}
])
with open(os.path.join(result_dir, 'salary_by_city.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(list(salary_by_city), indent=4, ensure_ascii=False))

salary_by_job = collection.aggregate([
    {"$group": {
        "_id": "$job",
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }},
    {"$sort": {"_id": 1}}
])
with open(os.path.join(result_dir, 'salary_by_job.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(list(salary_by_job), indent=4, ensure_ascii=False))

age_by_city = collection.aggregate([
    {"$group": {
        "_id": "$city",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }},
    {"$sort": {"_id": 1}}
])
with open(os.path.join(result_dir, 'age_by_city.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(list(age_by_city), indent=4, ensure_ascii=False))

age_by_job = collection.aggregate([
    {"$group": {
        "_id": "$job",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }},
    {"$sort": {"_id": 1}}
])
with open(os.path.join(result_dir, 'age_by_job.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(list(age_by_job), indent=4, ensure_ascii=False))

max_salary_min_age = collection.aggregate([
    {"$group": {
        "_id": None,
        "min_age": {"$min": "$age"}
    }},
    {"$lookup": {
        "from": "task_collection",
        "let": {"min_age": "$min_age"},
        "pipeline": [
            {"$match": {"$expr": {"$eq": ["$age", "$$min_age"]}}},
            {"$sort": {"salary": -1}},
            {"$limit": 1}
        ],
        "as": "max_salary_min_age"
    }}
])
with open(os.path.join(result_dir, 'max_salary_min_age.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(list(max_salary_min_age), indent=4, ensure_ascii=False))

min_salary_max_age = collection.aggregate([
    {"$group": {
        "_id": None,
        "max_age": {"$max": "$age"}
    }},
    {"$lookup": {
        "from": "task_collection",
        "let": {"max_age": "$max_age"},
        "pipeline": [
            {"$match": {"$expr": {"$eq": ["$age", "$$max_age"]}}},
            {"$sort": {"salary": 1}},
            {"$limit": 1}
        ],
        "as": "min_salary_max_age"
    }}
])
with open(os.path.join(result_dir, 'min_salary_max_age.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(list(min_salary_max_age), indent=4, ensure_ascii=False))

age_city_salary_50k = collection.aggregate([
    {"$match": {"salary": {"$gt": 50000}}},
    {"$group": {
        "_id": "$city",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }},
    {"$sort": {"avg_age": -1}}
])
with open(os.path.join(result_dir, 'age_city_salary_50k.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(list(age_city_salary_50k), indent=4, ensure_ascii=False))

ranges_salary = collection.aggregate([
    {"$match": {"$or": [{"age": {"$gt": 18, "$lt": 25}}, {"age": {"$gt": 50, "$lt": 65}}]}},
    {"$group": {
        "_id": {"city": "$city", "job": "$job"},
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }}
])
with open(os.path.join(result_dir, 'ranges_salary.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(list(ranges_salary), indent=4, ensure_ascii=False))

custom_query = collection.aggregate([
    {"$match": {"year": {"$gte": 2000}}},
    {"$group": {
        "_id": "$city",
        "avg_salary": {"$avg": "$salary"},
        "count": {"$sum": 1}
    }},
    {"$sort": {"avg_salary": -1}}
])
with open(os.path.join(result_dir, 'custom_query.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(list(custom_query), indent=4, ensure_ascii=False))
