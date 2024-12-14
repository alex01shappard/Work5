import msgpack
import json
from pymongo import MongoClient
from bson.json_util import dumps

def load_msgpack_to_mongo(file_path, collection):
    # Чтение данных из файла task_2.item.msgpack
    with open(file_path, 'rb') as file:
        data = msgpack.unpackb(file.read(), raw=False)
    # Добавление данных в коллекцию
    collection.insert_many(data)
    print("Данные успешно добавлены в MongoDB!")

def query_salary_stats(collection):
    pipeline = [
        {"$group": {
            "_id": None,
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }}
    ]
    results = list(collection.aggregate(pipeline))
    print("Статистика по зарплатам:", dumps(results, ensure_ascii=False, indent=4))

def query_jobs_count(collection):
    pipeline = [
        {"$group": {
            "_id": "$job",
            "count": {"$sum": 1}
        }}
    ]
    results = list(collection.aggregate(pipeline))
    print("Количество данных по профессиям:", dumps(results, ensure_ascii=False, indent=4))

def query_salary_by_city(collection):
    pipeline = [
        {"$group": {
            "_id": "$city",
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }}
    ]
    results = list(collection.aggregate(pipeline))
    print("Статистика по зарплатам в городах:", dumps(results, ensure_ascii=False, indent=4))

def query_salary_by_job(collection):
    pipeline = [
        {"$group": {
            "_id": "$job",
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }}
    ]
    results = list(collection.aggregate(pipeline))
    print("Статистика по зарплатам по профессиям:", dumps(results, ensure_ascii=False, indent=4))

def query_age_by_city(collection):
    pipeline = [
        {"$group": {
            "_id": "$city",
            "min_age": {"$min": "$age"},
            "avg_age": {"$avg": "$age"},
            "max_age": {"$max": "$age"}
        }}
    ]
    results = list(collection.aggregate(pipeline))
    print("Возрастная статистика по городам:", dumps(results, ensure_ascii=False, indent=4))

def query_age_by_job(collection):
    pipeline = [
        {"$group": {
            "_id": "$job",
            "min_age": {"$min": "$age"},
            "avg_age": {"$avg": "$age"},
            "max_age": {"$max": "$age"}
        }}
    ]
    results = list(collection.aggregate(pipeline))
    print("Возрастная статистика по профессиям:", dumps(results, ensure_ascii=False, indent=4))

def query_max_salary_min_age(collection):
    pipeline = [
        {"$sort": {"age": 1, "salary": -1}},
        {"$limit": 1}
    ]
    results = list(collection.aggregate(pipeline))
    print("Максимальная зарплата при минимальном возрасте:", dumps(results, ensure_ascii=False, indent=4))

def query_min_salary_max_age(collection):
    pipeline = [
        {"$sort": {"age": -1, "salary": 1}},
        {"$limit": 1}
    ]
    results = list(collection.aggregate(pipeline))
    print("Минимальная зарплата при максимальном возрасте:", dumps(results, ensure_ascii=False, indent=4))

def query_age_stats_salary_above_50k(collection):
    pipeline = [
        {"$match": {"salary": {"$gt": 50000}}},
        {"$group": {
            "_id": "$city",
            "min_age": {"$min": "$age"},
            "avg_age": {"$avg": "$age"},
            "max_age": {"$max": "$age"}
        }},
        {"$sort": {"avg_age": -1}}
    ]
    results = list(collection.aggregate(pipeline))
    print("Возрастная статистика по городам (зарплата > 50 000):", dumps(results, ensure_ascii=False, indent=4))

def query_salary_age_ranges(collection):
    pipeline = [
        {"$match": {
            "$or": [
                {"age": {"$gt": 18, "$lt": 25}},
                {"age": {"$gt": 50, "$lt": 65}}
            ]
        }},
        {"$group": {
            "_id": {"city": "$city", "job": "$job"},
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }}
    ]
    results = list(collection.aggregate(pipeline))
    print("Статистика зарплат по диапазонам возраста:", dumps(results, ensure_ascii=False, indent=4))

def custom_query(collection):
    pipeline = [
        {"$match": {"salary": {"$gte": 100000}}},
        {"$group": {
            "_id": "$city",
            "total_salary": {"$sum": "$salary"},
            "avg_age": {"$avg": "$age"}
        }},
        {"$sort": {"total_salary": -1}}
    ]
    results = list(collection.aggregate(pipeline))
    print("Произвольный запрос:", dumps(results, ensure_ascii=False, indent=4))

def main():
    # Подключение к MongoDB
    client = MongoClient(host='localhost', port=27017)
    db = client["db-2024"]
    collection = db.jobs
    
    # Загрузка данных из task_2.item.msgpack
    load_msgpack_to_mongo("data/task_2_item.msgpack", collection)
    
    # Выполнение запросов
    query_salary_stats(collection)
    query_jobs_count(collection)
    query_salary_by_city(collection)
    query_salary_by_job(collection)
    query_age_by_city(collection)
    query_age_by_job(collection)
    query_max_salary_min_age(collection)
    query_min_salary_max_age(collection)
    query_age_stats_salary_above_50k(collection)
    query_salary_age_ranges(collection)
    custom_query(collection)

if __name__ == "__main__":
    main()





