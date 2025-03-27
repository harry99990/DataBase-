from pymongo import MongoClient
from datetime import datetime

def get_db():
    # MongoDB에 연결 
    client = MongoClient('mongodb://localhost:27017/')
    db = client.local  # 'local' 데이터베이스 사용
    return db, client

def insert_data(db):
    # 책 데이터 삽입
    books = [
        {"title": "Kafka on the Shore", "author": "Haruki Murakami", "year": 2002},
        {"title": "Norwegian Wood", "author": "Haruki Murakami", "year": 1987},
        {"title": "1Q84", "author": "Haruki Murakami", "year": 2009}
    ]
    db.books.insert_many(books)

    # 영화 데이터 삽입
    movies = [
        {"title": "Inception", "director": "Christopher Nolan", "year": 2010, "rating": 8.8},
        {"title": "Interstellar", "director": "Christopher Nolan", "year": 2014, "rating": 8.6},
        {"title": "The Dark Knight", "director": "Christopher Nolan", "year": 2008, "rating": 9.0}
    ]
    db.movies.insert_many(movies)

    # 사용자 행동 데이터 삽입
    user_actions = [
        {"user_id": 1, "action": "click", "timestamp": datetime(2023, 4, 12, 8, 0)},
        {"user_id": 1, "action": "view", "timestamp": datetime(2023, 4, 12, 9, 0)},
        {"user_id": 2, "action": "purchase", "timestamp": datetime(2023, 4, 12, 10, 0)},
    ]
    db.user_actions.insert_many(user_actions)

    print("Data inserted successfully")



# 문제 1.특정 장르의 책 찾기
def find_books_by_genre(db):
    books = db.books.find({"genre": "fantasy"}, {"_id": 0, "title": 1, "author": 1})
    
    for book in books:
        print(book)

# 문제 2. 감독별 평균 영화 평점 계산
def calculate_average_ratings(db):
    ranks = db.movies.aggregate([
        {"$group": {"_id": "$director", "average_rating": {"$avg": "$rating"}}}
    , {"$sort": {"average_rating": -1}}
    ])

    for rank in ranks:
        print(rank)

# 문제 3. 특정 사용자의 최근 행동 조회
def find_recent_actions_by_user(db, user_id):
    actions = db.user_actions.find({"user_id": user_id}).sort([("timestamp", -1)]).limit(5) # 리스트(배열) 형식으로 전달 

    for action in actions:
        print(action)

# 문제 4. 출판 연도별 책의 수 계산
def count_books_by_year(db):
    cals = db.books.aggregate([
        {"$group": {"_id": "$year", "count": {"$sum" : 1}}},
        {"$sort": {"count": -1}}
    ])

    for cal in cals:
        print(cal)

# 문제 5. 특정 사용자의 행동 유형 업데이트
from datetime import datetime

def update_user_actions_before_date(db, user_id):
    result = db.user_actions.update_many(
        {"user_id": user_id, "action": "view", "timestamp": {"$lt": datetime(2023, 4, 13)}},
        {"$set": {"action": "seen"}}
    )
    print(f"Updated {result.modified_count} documents.")

# UpdateResult 객체 속성

def drop_collections(db):    
    collections = db.list_collection_names()
    for collection in collections:
        db.drop_collection(collection)

if __name__ == "__main__":
    db, client = get_db()
    drop_collections(db)
    insert_data(db)
    find_books_by_genre(db)
    calculate_average_ratings(db)
    find_recent_actions_by_user(db, 1)
    count_books_by_year(db)
    update_user_actions_before_date(db, 1)

    client.close()