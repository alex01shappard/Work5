import csv
from pymongo import MongoClient

# Функция для загрузки данных из csv-файла
def load_csv_to_mongo():
    # Подключение к MongoDB
    client = MongoClient(host='localhost', port=27017)
    db = client["db-2024"]
    jobs_collection = db.jobs

    # Чтение данных из csv
    with open("data/task_3_item.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=';')  # Используем ";" как разделитель
        data = [
            {
                "job": row["job"],
                "salary": int(row["salary"]),
                "id": int(row["id"]),
                "city": row["city"],
                "year": int(row["year"]),
                "age": int(row["age"]),
            }
            for row in reader
        ]
        # Добавляем данные в коллекцию
        jobs_collection.insert_many(data)

    print("Данные из нашего csv файла успешно загружены в MongoDB!")

# Запрос 1: Удалить документы по предикату (salary < 25000 || salary > 175000)
def delete_by_salary(jobs_collection):
    result = jobs_collection.delete_many({"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]})
    print(f"Удалено документов: {result.deleted_count}")

# Запрос 2: Увеличить возраст всех документов на 1
def increase_age(jobs_collection):
    result = jobs_collection.update_many({}, {"$inc": {"age": 1}})
    print(f"Обновлено документов: {result.modified_count}")

# Запрос 3: Поднять зарплату на 5% для произвольно выбранных профессий
def increase_salary_by_job(jobs_collection, jobs):
    result = jobs_collection.update_many({"job": {"$in": jobs}}, {"$mul": {"salary": 1.05}})
    print(f"Обновлено документов: {result.modified_count}")

# Запрос 4: Поднять зарплату на 7% для произвольно выбранных городов
def increase_salary_by_city(jobs_collection, cities):
    result = jobs_collection.update_many({"city": {"$in": cities}}, {"$mul": {"salary": 1.07}})
    print(f"Обновлено документов: {result.modified_count}")

# Запрос 5: Поднять зарплату на 10% для сложного предиката
def increase_salary_complex(jobs_collection, city, jobs, age_range):
    result = jobs_collection.update_many(
        {
            "city": city,
            "job": {"$in": jobs},
            "age": {"$gte": age_range[0], "$lte": age_range[1]},
        },
        {"$mul": {"salary": 1.10}},
    )
    print(f"Обновлено документов: {result.modified_count}")

# Запрос 6: Удалить документы по произвольному предикату
def delete_by_custom_predicate(jobs_collection, predicate):
    result = jobs_collection.delete_many(predicate)
    print(f"Удалено документов: {result.deleted_count}")

# Основная функция
def main():
    client = MongoClient(host='localhost', port=27017)
    db = client["db-2024"]
    jobs_collection = db.jobs

    # Загрузка данных
    load_csv_to_mongo()

    # Выполнение запросов
    print("Запрос 1: Удаление документов по зарплате")
    delete_by_salary(jobs_collection)

    print("\nЗапрос 2: Увеличение возраста всех документов на 1")
    increase_age(jobs_collection)

    print("\nЗапрос 3: Поднятие зарплаты на 5% для профессий ['Программист', 'Инженер']")
    increase_salary_by_job(jobs_collection, ["Программист", "Инженер"])

    print("\nЗапрос 4: Поднятие зарплаты на 7% для городов ['Москва', 'Санкт-Петербург']")
    increase_salary_by_city(jobs_collection, ["Москва", "Санкт-Петербург"])

    print("\nЗапрос 5: Поднятие зарплаты на 10% для сложного предиката")
    increase_salary_complex(jobs_collection, "Москва", ["Программист", "Учитель"], (25, 35))

    print("\nЗапрос 6: Удаление документов по произвольному предикату (age > 60)")
    delete_by_custom_predicate(jobs_collection, {"age": {"$gt": 60}})

if __name__ == "__main__":
    main()
