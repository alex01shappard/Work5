from pymongo import MongoClient
import json
import csv

# Функция для подключения к MongoDB
def connect_db():
    # Подключаемся к MongoDB на локальном хосте
    client = MongoClient('localhost', 27017)
    # Указываем базу данных
    db = client['new-db-2024']
    # Указываем коллекцию films
    films_collection = db['films']
    return films_collection

# Функция для загрузки данных из json
def load_json_data(file_path, collection):
    with open(file_path, 'r', encoding='utf-8') as file:
        # Загружаем данные из json файла
        data = json.load(file)
        # Извлекаем список фильмов
        movies = data["movies"]
        # Вставляем данные в коллекцию MongoDB
        collection.insert_many(movies)

# Функция для загрузки данных из CSV
def load_csv_data(file_path, collection):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = [row for row in reader]
        # Вставляем данные в коллекцию MongoDB
        collection.insert_many(rows)

# Подключаемся к базе данных
films_collection = connect_db()

# Загружаем данные из файлов
load_json_data('data/palestinian_movies.json', films_collection)
print("Данные из palestinian_movies.json успешно загружены!")

load_csv_data('data/movie_statistic_dataset.csv', films_collection)
print("Данные из movie_statistic_dataset.csv успешно загружены!")

# Задание 1: Запросы по выборке
print("\nЗапрос 1: Все фильмы, выпущенные после 2010 года")
result = films_collection.find({"releaseDate.year": {"$gt": 2010}})
for movie in result:
    print(movie)

print("\nЗапрос 2: Фильмы, жанр которых включает 'Action'")
result = films_collection.find({"genres": {"$regex": "Action"}})
for movie in result:
    print(movie)

print("\nЗапрос 3: Фильмы с рейтингом больше 8")
result = films_collection.find({"movie_averageRating": {"$gt": 8}})
for movie in result:
    print(movie)

print("\nЗапрос 4: Фильмы с режиссером 'James Cameron'")
result = films_collection.find({"director_name": "James Cameron"})
for movie in result:
    print(movie)

print("\nЗапрос 5: Фильмы, выпущенные в США")
result = films_collection.find({"releaseDate.country.text": "United States"})
for movie in result:
    print(movie)

# Задание 2: Запросы с агрегацией
print("\nЗапрос 1: Средний рейтинг по жанрам")
pipeline = [
    {"$unwind": "$genres"},
    {"$group": {"_id": "$genres", "avg_rating": {"$avg": "$movie_averageRating"}}}
]
result = films_collection.aggregate(pipeline)
for res in result:
    print(res)

print("\nЗапрос 2: Количество фильмов по годам выпуска")
pipeline = [
    {"$group": {"_id": "$releaseDate.year", "count": {"$sum": 1}}}
]
result = films_collection.aggregate(pipeline)
for res in result:
    print(res)

print("\nЗапрос 3: Общий бюджет фильмов по жанрам")
pipeline = [
    {"$unwind": "$genres"},
    {"$group": {"_id": "$genres", "total_budget": {"$sum": "$Production budget $"}}}
]
result = films_collection.aggregate(pipeline)
for res in result:
    print(res)

print("\nЗапрос 4: Среднее время для фильмов с рейтингом выше 7")
pipeline = [
    {"$match": {"movie_averageRating": {"$gt": 7}}},
    {"$group": {"_id": None, "avg_runtime": {"$avg": "$runtime_minutes"}}}
]
result = films_collection.aggregate(pipeline)
for res in result:
    print(res)

print("\nЗапрос 5: Фильмы с самым высоким бюджетом")
pipeline = [
    {"$sort": {"Production budget $": -1}},
    {"$limit": 1}
]
result = films_collection.aggregate(pipeline)
for res in result:
    print(res)

# Задание 3: Обновление и удаление данных
# Запрос 1: Обновить бюджет фильмов с рейтингом ниже 7
films_collection.update_many(
    {"movie_averageRating": {"$lt": 7}},
    {"$set": {"Production budget $": 500000000}}
)
print("\nОбновлено бюджетов для фильмов с рейтингом ниже 7")

# Запрос 2: Удалить фильмы с рейтингом ниже 6
films_collection.delete_many({"movie_averageRating": {"$lt": 6}})
print("Удалены фильмы с рейтингом ниже 6")

# Запрос 3: Обновить время для всех фильмов, жанр которых 'Action'
films_collection.update_many(
    {"genres": {"$regex": "Action"}},
    {"$set": {"runtime_minutes": 120}}
)
print("Обновлено время для фильмов жанра 'Action'")

# Запрос 4: Удалить все фильмы, выпущенные до 2000 года
films_collection.delete_many({"releaseDate.year": {"$lt": 2000}})
print("Удалены фильмы, выпущенные до 2000 года")

# Запрос 5: Увеличить бюджет на 10% для всех фильмов, выпущенных после 2015 года
films_collection.update_many(
    {"releaseDate.year": {"$gt": 2015}},
    {"$mul": {"Production budget $": 1.10}}
)
print("Увеличен бюджет на 10% для фильмов, выпущенных после 2015 года")



