import json
from pymongo import MongoClient
from bson.json_util import dumps  # Импортируем bson.json_util.dumps для сериализации

def load_data_to_mongo():
    # Подключаемся к MongoDB
    client = MongoClient(host='localhost', port=27017)
    db = client["db-2024"]  # Указываем базу данных
    jobs_collection = db.jobs  # Указываем коллекцию
    
    # Очищаем коллекцию перед загрузкой
    jobs_collection.delete_many({})
    
    # Читаем данные из json файла
    with open("data/task_1_item.json", "r", encoding="utf-8") as file:
        data = json.load(file)  # Загружаем данные из файла
        jobs_collection.insert_many(data)  # Записываем данные в MongoDB
    
    print("Данные успешно загружены в MongoDB!")

# Запрос 1: Вывод первых 10 записей, отсортированных по убыванию по полю salary
def query_1(jobs_collection):
    results = jobs_collection.find().sort("salary", -1).limit(10)
    print(dumps(results, ensure_ascii=False, indent=4))  # Используем bson.json_util.dumps

# Запрос 2: Вывод первых 15 записей, отфильтрованных по предикату age < 30, отсортировать по убыванию по полю salary    
def query_2(jobs_collection):
    results = jobs_collection.find({"age": {"$lt": 30}}).sort("salary", -1).limit(15)
    print(dumps(results, ensure_ascii=False, indent=4))  # Используем bson.json_util.dumps

# Запрос 3: Вывод первых 10 записей по сложному предикату
def query_3(jobs_collection):
    results = jobs_collection.find({
        "city": "Ташкент",
        "job": {"$in": ["Программист", "Учитель", "Инженер"]}
    }).sort("age", 1).limit(10)
    print(dumps(results, ensure_ascii=False, indent=4))  # Используем bson.json_util.dumps

# Запрос 4: Подсчет записей по сложному предикату
def query_4(jobs_collection):
    results_count = jobs_collection.count_documents({
        "$and": [
            {"age": {"$gte": 20, "$lte": 40}},
            {"year": {"$gte": 2019, "$lte": 2022}},
            {"$or": [
                {"salary": {"$gt": 50000, "$lte": 75000}},
                {"salary": {"$gt": 125000, "$lt": 150000}}
            ]}
        ]
    })
    print(f"Количество записей: {results_count}")

# Объединение всех функций
def main():
    client = MongoClient(host='localhost', port=27017)
    db = client["db-2024"]
    jobs_collection = db.jobs
    
    print("Запрос 1:")
    query_1(jobs_collection)
    
    print("\nЗапрос 2:")
    query_2(jobs_collection)
    
    print("\nЗапрос 3:")
    query_3(jobs_collection)
    
    print("\nЗапрос 4:")
    query_4(jobs_collection)

if __name__ == "__main__":
    load_data_to_mongo()  # Загружаем данные в MongoDB
    main()  # Выполняем запросы
