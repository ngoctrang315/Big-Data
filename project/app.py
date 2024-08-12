from unittest import result
from fastapi import FastAPI
import recommender
import atexit

#global variables
uri = "bolt://localhost:7687"
user = "neo4j"
password = "12345678" # modify this accordingly based on your own password
    
neo_db = recommender.Recommender(uri, user, password)

app = FastAPI()


# Top 10 phim 
@app.post('/top10Movies')
async def find_top10_movies():
    result = neo_db.findTop10Movies()
    return result

# Tạo user với tên và id
@app.post('/createNewUser')
async def create_a_new_user(name, custom_id):
    result = neo_db.createUser(name, custom_id)
    return result

# Cập Nhật tên user với id
@app.post('/updateUser')
async def update_a_user(name, custom_id):
    result = neo_db.updateUser(name, custom_id)
    return result

# Tạo relationship rating giữa user và movie
@app.post('/ratingMovie')
async def rating_a_movie(userId, movieId, rating):
    result = neo_db.ratedMovie(userId, movieId, rating)
    return result

# Delete relationship user rating movie
@app.delete('/deleteRating')
async def delete_rating(userId, movieId):
    result = neo_db.deleteRating(userId, movieId)
    return result

# Delete User (Node)
@app.delete('/deleteUser')
async def delete_User(userId):
    result = neo_db.deleteUser(userId)
    return result

# Tìm bộ phim có chưa chuỗi được nhập vào
@app.get('/getMovie')
async def find_movie_by_name(movie_name):
    result = neo_db.findMovie(movie_name)
    return result

# Tìm bộ phim, đạo diễn và diễn phim bằng id phim
@app.get('/getMovieById')
async def find_movie_by_Id(id_movie):
    result = neo_db.findMovieById(id_movie)
    return result

# giới thiệu phim mà user chưa xem
@app.get('/get_recommend_user')
async def find_recommendation_by_user(user_name):
    result = neo_db.findRecommendationUser(user_name)
    return result
