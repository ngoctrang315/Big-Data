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
    
    # a simple example from database
    def find_movie(self, movie_name):
        with self.driver.session() as session:
            result = session.read_transaction(self.__find_and_return_movie, movie_name)
            return {"movie": result}

    def find_movie_by_id(self, id_movie):
        with self.driver.session() as session:
            result = session.read_transaction(self.__find_id_and_return_movie, id_movie)
            return {"movie": result}        

    def find_top10movies(self):
        with self.driver.session() as session:
            result = session.read_transaction(self.__find_top10_and_return_movie)
            return {"movie": result}

    # create a new user with name and custom ID
    def createUser(self, name, id):
        with self.driver.session() as session:
            session.write_transaction(self.__create_user, name, id)
            return {"result": "OK"}

    # update a user with name and custom ID
    def updateUser(self, name, id):
        with self.driver.session() as session:
            session.write_transaction(self.__update_user, name, id)
            return {"result": "OK"}

    # make user can rate for a movie
    def ratedMovie(self, userId, movieId, rating):
        with self.driver.session() as session:
            result = session.write_transaction(self.__make_rating_movie, userId, movieId, rating)
            return {"result": result}
        
    # release a movie from a user's rating 
    def deleteRating(self, userId, movieId):
        with self.driver.session() as session:
            result = session.write_transaction(self.__delete_rating, userId, movieId)
            return {"result": "DELETED"}

    # delete user
    def deleteUser(self, userId):
        with self.driver.session() as session:
            result = session.write_transaction(self.__delete_user, userId)
            return {"result": "DELETED"}

    #Recommend movies similar to those the user has already watched
    def find_recommendation_user(self, username):
        with self.driver.session() as session:
            result = session.read_transaction(self.__find_by_user_watch, username)
            return {"result": result}
    
    # kNN movie recommendation using Pearson similarity
    def find_recommendation_knn(self, username):
        with self.driver.session() as session:
            result = session.read_transaction(self.__find_content_knn, username)
            return {"result": result}

    @staticmethod
    def __find_content_knn(tx, username):
        query = ("""MATCH (u1:User {name: $username})-[r:RATED]->(m:Movie)
                    WITH u1, avg(r.rating) AS u1_mean
                    MATCH (u1)-[r1:RATED]->(m:Movie)<-[r2:RATED]-(u2)
                    WITH u1, u1_mean, u2, COLLECT({r1: r1, r2: r2}) AS ratings 
                    WHERE size(ratings) > 10
                    MATCH (u2)-[r:RATED]->(m:Movie)
                    WITH u1, u1_mean, u2, avg(r.rating) AS u2_mean, ratings
                    UNWIND ratings AS r
                    WITH sum( (r.r1.rating-u1_mean) * (r.r2.rating-u2_mean) ) AS nom,
                    sqrt(sum((r.r1.rating - u1_mean)^2) * sum((r.r2.rating - u2_mean)^2)) AS denom, u1, u2 
                    WHERE denom <> 0
                    WITH u1, u2, nom/denom AS pearson
                    ORDER BY pearson DESC LIMIT 10
                    MATCH (u2)-[r:RATED]->(m:Movie) WHERE NOT EXISTS( (u1)-[:RATED]->(m) )
                    RETURN m.title as recommendation, SUM( pearson * r.rating) AS score
                    ORDER BY score DESC LIMIT 15""")
        try:
            result = tx.run(query, username = username)
            return  [{record['recommendation']:record['score']} for record in result]
        except ServiceUnavailable as exception:
            logging.error("Recommend movies by KNN: {query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise 
    @staticmethod
    def __find_by_user_watch(tx, username):

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
    @staticmethod
    def __delete_rating(tx, userId, movieId):
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
    @staticmethod
    def __delete_user(tx, userId):
        query = ("""MATCH (n:User) WHERE n.userId= $userId DELETE n """)
        try:
            result = tx.run(query, userId=userId)
            return [record for record in result]
        except ServiceUnavailable as exception:
            logging.error("delete rating: {query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise          
    @staticmethod
    def __make_rating_movie(tx, userId, movieId, rating):
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
    @staticmethod
    def __update_user(tx, name_user, userID):
        query = ('''MATCH (n:User)
                    WHERE n.userId = $userID 
                    SET n.name = $name_user ''')
        try:
            tx.run(query, name_user = name_user, userID = userID).data()
            return "Updated"
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    @staticmethod
    def __create_user(tx, name_user, userID):
        query = ('''CREATE (:User {name: $name_user, userId: $userID})''')
        try:
            tx.run(query, name_user = name_user, userID = userID).data()
            return "Created"
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise
    @staticmethod
    def __find_and_return_movie(tx, movie_name):
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
    @staticmethod
    def __find_top10_and_return_movie(tx):
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
    @staticmethod
    def __find_id_and_return_movie(tx, id_movie):
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
        
# for testing purpose, can be commented out
if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "12345678"
