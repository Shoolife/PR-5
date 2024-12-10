import os
import json
from pymongo import MongoClient
from bson import json_util

client = MongoClient('mongodb://localhost:27017/')
db = client['task_database']
collection = db['task_collection']

file_path = 'task_1_item.json'

collection.delete_many({})

with open(file_path, 'r', encoding='utf-8') as file:
    data = json.load(file)
    collection.insert_many(data)

result_dir = 'result'
os.makedirs(result_dir, exist_ok=True)

top_10_by_salary = list(collection.find().sort('salary', -1).limit(10))
with open(os.path.join(result_dir, 'top_10_by_salary.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(top_10_by_salary, indent=4, ensure_ascii=False))

top_15_age_less_30 = list(
    collection.find({"age": {"$lt": 30}})
    .sort('salary', -1)
    .limit(15)
)
with open(os.path.join(result_dir, 'top_15_age_less_30.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(top_15_age_less_30, indent=4, ensure_ascii=False))

chosen_city = 'Мадрид'
chosen_jobs = ['Программист', 'IT-специалист', 'Бухгалтер']

filtered_city_and_jobs = list(
    collection.find(
        {"$and": [
            {"city": chosen_city},
            {"job": {"$in": chosen_jobs}}
        ]}
    )
    .sort('age', 1)
    .limit(10)
)
with open(os.path.join(result_dir, 'filtered_city_and_jobs.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(filtered_city_and_jobs, indent=4, ensure_ascii=False))

age_range = {"$gte": 25, "$lte": 40}  
filtered_count = collection.count_documents({
    "$and": [
        {"age": age_range},
        {"year": {"$gte": 2019, "$lte": 2022}},
        {"$or": [
            {"salary": {"$gt": 50000, "$lte": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}}
        ]}
    ]
})
with open(os.path.join(result_dir, 'filtered_count.json'), 'w', encoding='utf-8') as file:
    json.dump({"filtered_count": filtered_count}, file, indent=4, ensure_ascii=False)
