from pickle import TRUE
from unittest import result
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

from datetime import datetime


class Recommender:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Close the driver connection when finished
        self.driver.close() 

    # Tìm top10 phim được xếp theo doanh thu giảm dần
    def findTop10Movies(self):
        with self.driver.session() as session:
            result = session.read_transaction(self.__findTop10AndReturnMovie)
            return {"movie": result}
        
    @staticmethod
    #Tìm top10 phim theo doanh thu giảm dần
    def __findTop10AndReturnMovie(tx):
        query = (
            """MATCH (n:Movie) 
                RETURN n.title 
                ORDER BY -n.revenue  LIMIT 10"""
        )
        try:
            result = tx.run(query)
            return [ {record["n.title"]} for record in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    # Tạo 1 User mới
    def createUser(self, name, id):
        with self.driver.session() as session:
            session.write_transaction(self.__createUser, name, id)
            return {"result": "OK"}
        
    @staticmethod
    def __createUser(tx, name_user, userID):
        query = ('''CREATE (:User {name: $name_user, userId: $userID})''')
        try:
            tx.run(query, name_user = name_user, userID = userID).data()
            return "Created"
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    # Chỉnh Sửa thông tin User
    def updateUser(self, name, id):
        with self.driver.session() as session:
            session.write_transaction(self.__updateUser, name, id)
            return {"result": "OK"}
        
    @staticmethod
    def __updateUser(tx, name_user, userID):
        query = ('''MATCH (n:User)
                    WHERE n.userId = $userID 
                    SET n.name = $name_user ''')
        try:
            tx.run(query, name_user = name_user, userID = userID).data()
            return "Updated"
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    # Tạo quan hệ User đánh giá một bộ phim theo Id phim
    def ratedMovie(self, userId, movieId, rating):
        with self.driver.session() as session:
            result = session.write_transaction(self.__makeRatingMovie, userId, movieId, rating)
            return {"result": result}
        
    @staticmethod
    def __makeRatingMovie(tx, userId, movieId, rating):
        # current date and time
        now = datetime.now()
        # time is now
        timestamp = round(datetime.timestamp(now))
        query = ("""MATCH(u:User),(m:Movie)
                WHERE u.userId = $userId AND m.movieId = $movieId
                CREATE (u)-[r:RATED {rating: $rating  , timestamp:$stamp}]->(m)
                RETURN type(r) as relation , r.rating as rating""")
        try:
            result = tx.run(query, userId=userId, movieId=movieId, rating = rating, stamp = timestamp)
            return [ {record["relation"]: record["rating"]} for record in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise
        
    # Xóa quan hệ đánh giá 1 bộ phim của User 
    def deleteRating(self, userId, movieId):
        with self.driver.session() as session:
            result = session.write_transaction(self.__deleteRating, userId, movieId)
            return {"result": "DELETED"}
        
    @staticmethod
    def __deleteRating(tx, userId, movieId):
        query = ("""MATCH (u:User)-[r:RATED]->(m:Movie)
                    WHERE u.userId = $userId AND m.movieId = $movieId
                    DELETE r
                """)
        try:
            result = tx.run(query, userId=userId, movieId = movieId)
            return [record for record in result]
        except ServiceUnavailable as exception:
            logging.error("delete rating: {query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    # Xóa 1 User
    def deleteUser(self, userId):
        with self.driver.session() as session:
            result = session.write_transaction(self.__deleteUser, userId)
            return {"result": "DELETED"}
        
    @staticmethod
    def __deleteUser(tx, userId):
        query = ("""MATCH (n:User) WHERE n.userId= $userId DETACH DELETE n """)
        try:
            result = tx.run(query, userId=userId)
            return [record for record in result]
        except ServiceUnavailable as exception:
            logging.error("delete rating: {query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise  

    # Tìm phim theo 1 chuỗi
    def findMovie(self, movie_name):
        with self.driver.session() as session:
            result = session.read_transaction(self.__findAndReturnMovie, movie_name)
            return {"movie": result}
        
    @staticmethod
    def __findAndReturnMovie(tx, movie_name):
        query = (
            """MATCH (m:Movie)<-[:RATED]-(u:User)
            WHERE m.title CONTAINS $movie_name
            WITH m, COUNT(*) AS reviews
            RETURN m.title AS movie, reviews
            ORDER BY reviews DESC LIMIT 5;"""
        )
        try:
            result = tx.run(query, movie_name=movie_name)
            return [ {record["movie"]: record["reviews"]} for record in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    # Tìm phim theo Id của phim
    def findMovieById(self, id_movie):
        with self.driver.session() as session:
            result = session.read_transaction(self.__findIdAndReturnMovie, id_movie)
            return {"movie": result}     

    @staticmethod
    def __findIdAndReturnMovie(tx, id_movie):
        query = (
            """MATCH (d:Director)-[:DIRECTED]->(n:Movie)<-[:ACTED_IN]-(a:Actor) 
                WHERE n.movieId = $id_movie RETURN n,a,d """
        )
        try:
            result = tx.run(query,  id_movie=id_movie)
            return [ {"movie": record["n"], "actor": record["a"], "director": record["d"]} for record in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise  

    # Tìm những bộ phim mà User chưa xem
    def findRecommendationUser(self, username):
        with self.driver.session() as session:
            result = session.read_transaction(self.__findByUserWatch, username)
            return {"result": result}
    
    @staticmethod
    def __findByUserWatch(tx, username):
        query = ("""MATCH (u:User {name: $username})-[:RATED]->(:Movie)<-[:RATED]-(o:User), (o)-[:RATED]->(rec:Movie)
                    WHERE NOT EXISTS( (u)-[:RATED]->(rec) )
                    RETURN rec.title as title, rec.year as year
                    LIMIT 25""")
        try:
            result = tx.run(query, username = username)
            return  [ {record['title']:record['year']} for record in result]
        except ServiceUnavailable as exception:
            logging.error("Recommend movies by user watched: {query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise 
