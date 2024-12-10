import os
from pymongo import MongoClient
from bson import json_util

client = MongoClient('mongodb://localhost:27017/')
db = client['task_database']
collection = db['task_collection']

file_path = 'task_3_item.text'

def parse_custom_format(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        entry = {}
        for line in file:
            line = line.strip()
            if line == '=====':  
                data.append(entry)
                entry = {}
            elif line:  
                key, value = line.split('::')
                entry[key.strip()] = int(value.strip()) if value.strip().isdigit() else value.strip()
        if entry:  
            data.append(entry)
    return data

data_task_3 = parse_custom_format(file_path)
collection.insert_many(data_task_3)

result_dir = 'result'
os.makedirs(result_dir, exist_ok=True)

initial_data = list(collection.find())
with open(os.path.join(result_dir, 'initial_data.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(initial_data, indent=4, ensure_ascii=False))

collection.delete_many({"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]})
remaining_after_deletion = list(collection.find())
with open(os.path.join(result_dir, 'after_deletion.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(remaining_after_deletion, indent=4, ensure_ascii=False))

collection.update_many({}, {"$inc": {"age": 1}})
remaining_after_age_increment = list(collection.find())
with open(os.path.join(result_dir, 'after_age_increment.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(remaining_after_age_increment, indent=4, ensure_ascii=False))

selected_jobs = ['Программист', 'Учитель', 'Водитель']
collection.update_many({"job": {"$in": selected_jobs}}, {"$mul": {"salary": 1.05}})
remaining_after_job_salary_update = list(collection.find())
with open(os.path.join(result_dir, 'after_job_salary_update.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(remaining_after_job_salary_update, indent=4, ensure_ascii=False))

selected_cities = ['Москва', 'Минск', 'Алма-Ата']
collection.update_many({"city": {"$in": selected_cities}}, {"$mul": {"salary": 1.07}})
remaining_after_city_salary_update = list(collection.find())
with open(os.path.join(result_dir, 'after_city_salary_update.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(remaining_after_city_salary_update, indent=4, ensure_ascii=False))

complex_predicate = {
    "$and": [
        {"city": "Санкт-Петербург"},
        {"job": {"$in": ["Врач", "Психолог"]}},
        {"age": {"$gte": 30, "$lte": 50}}
    ]
}
collection.update_many(complex_predicate, {"$mul": {"salary": 1.10}})
remaining_after_complex_predicate = list(collection.find())
with open(os.path.join(result_dir, 'after_complex_predicate.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(remaining_after_complex_predicate, indent=4, ensure_ascii=False))

delete_predicate = {"year": {"$lt": 2000}}  
collection.delete_many(delete_predicate)
remaining_after_final_deletion = list(collection.find())
with open(os.path.join(result_dir, 'after_final_deletion.json'), 'w', encoding='utf-8') as file:
    file.write(json_util.dumps(remaining_after_final_deletion, indent=4, ensure_ascii=False))

