import os
import boto3
# import numpy as np
# import pandas as pd
# from io import StringIO
# from gensim.models import Word2Vec
from datetime import date, datetime
from dotenv import load_dotenv
from decimal import Decimal
from flask import Flask, request, jsonify
from flask_restful import Api, Resource
# from fuzzywuzzy import process
# import re
import pymysql
from werkzeug.exceptions import BadRequest, InternalServerError


app = Flask(__name__)
api = Api(app)


# --------------- DATABASE CONFIGUATION ------------------
# Connect to MySQL database (API v2)
def connect():
    # global RDS_PW
    # global RDS_HOST
    # global RDS_PORT
    # global RDS_USER
    # global RDS_DB

    print("Trying to connect to RDS (API v2)...")
    # print("RDS_HOST: ", os.getenv('RDS_HOST'))
    # print("RDS_USER: ", os.getenv('RDS_USER'))
    # print("RDS_PORT: ", os.getenv('RDS_PORT'), type(os.getenv('RDS_PORT')))
    # print("RDS_PW: ", os.getenv('RDS_PW'))
    # print("RDS_DB: ", os.getenv('RDS_DB'))

   
    try:
        conn = pymysql.connect(
            host=os.getenv('RDS_HOST'),
            user=os.getenv('RDS_USER'),
            port=int(os.getenv('RDS_PORT')),
            passwd=os.getenv('RDS_PW'),
            db=os.getenv('RDS_DB'),
            cursorclass=pymysql.cursors.DictCursor,
        )
        print("Successfully connected to RDS. (API v2)")
        return conn
    except:
        print("Could not connect to RDS. (API v2)")
        raise Exception("RDS Connection failed. (API v2)")


# Disconnect from MySQL database (API v2)
def disconnect(conn):
    try:
        conn.close()
        print("Successfully disconnected from MySQL database. (API v2)")
    except:
        print("Could not properly disconnect from MySQL database. (API v2)")
        raise Exception("Failure disconnecting from MySQL database. (API v2)")
    

# Serialize JSON
def serializeResponse(response):
    try:
        # print("In Serialize JSON")
        for row in response:
            for key in row:
                if type(row[key]) is Decimal:
                    row[key] = float(row[key])
                elif type(row[key]) is date or type(row[key]) is datetime:
                    row[key] = row[key].strftime("%Y-%m-%d")
        # print("In Serialize JSON response", response)
        return response
    except:
        raise Exception("Bad query JSON")


# Execute an SQL command (API v2)
# Set cmd parameter to 'get' or 'post'
# Set conn parameter to connection object
# OPTIONAL: Set skipSerialization to True to skip default JSON response serialization
def execute(sql, cmd, conn, skipSerialization=False):
    response = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cmd == "get":
                result = cur.fetchall()
                response["message"] = "Successfully executed SQL query."
                # Return status code of 280 for successful GET request
                response["code"] = 280
                if not skipSerialization:
                    result = serializeResponse(result)
                response["result"] = result
            elif cmd == "post":
                conn.commit()
                response["message"] = "Successfully committed SQL command."
                # Return status code of 281 for successful POST request
                response["code"] = 281
            else:
                response[
                    "message"
                ] = "Request failed. Unknown or ambiguous instruction given for MySQL command."
                # Return status code of 480 for unknown HTTP method
                response["code"] = 480
    except:
        response["message"] = "Request failed, could not execute MySQL command."
        # Return status code of 490 for unsuccessful HTTP request
        response["code"] = 490
    finally:
        response["sql"] = sql
        return response

# -- Endpoints start here -------------------------------------------------------------------------------

class AddMovieRating(Resource):
    def post(self):
        data = request.get_json()
        user_id = data.get('user_id')
        movie_id = data.get('movie_id')
        rating = data.get('rating')

        if not user_id or not movie_id or not rating:
            return {"error": "user_id, movie_id, and rating are required"}, 400

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                "INSERT INTO user_ratings (user_id, movie_id, rating) VALUES (%s, %s, %s)",
                (user_id, movie_id, rating)
            )
            conn.commit()
        except mysql.connector.Error as err:
            return {"error": str(err)}, 500
        finally:
            cursor.close()
            conn.close()

        return {"message": "Rating added successfully"}, 201


def get_model_from_s3():
    s3_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    s3_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    s3_bucket_name = os.getenv('BUCKET_NAME')
    s3_file_key_word2vec_model = os.getenv('S3_PATH_KEY_WORD2VEC_MODEL')

    s3_client = boto3.client('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)
    # after s3 connect

    local_model_path = '/tmp/word2vec_movie_ratings_embeddings.model'
    s3_client.download_file(s3_bucket_name, s3_file_key_word2vec_model, local_model_path)
    # model downloaded from s3

    model = Word2Vec.load(local_model_path)
    return model


def get_genres_from_s3():
    s3_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    s3_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    s3_bucket_name = os.getenv('BUCKET_NAME')
    s3_file_key_genres = os.getenv('S3_PATH_KEY_GENRES')

    s3_client = boto3.client('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)

    genres_response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_key_genres)

    genres_csv_content = genres_response['Body'].read().decode('utf-8')
    genres_cleaned = pd.read_csv(StringIO(genres_csv_content))
    return genres_cleaned


def generate_user_profile(ratings, model):
    user_vector = np.zeros(model.vector_size)
    total_weight = 0.0

    for movie_id, rating in ratings.items():
        if str(movie_id) in model.wv:
            user_vector += model.wv[str(movie_id)] * rating
            total_weight += rating

    if total_weight > 0:
        user_vector /= total_weight

    return user_vector


def search(title):
    movies = get_genres_from_s3()
    matching_movies = movies[movies['title'].str.contains(
        title, case=False, na=False)]
    return matching_movies[['movieId', 'title', 'genres']]  # return results



def recommend_movies(user_vector, model, metadata, top_n=10):
    similarity_scores = []

    for movie_id in model.wv.index_to_key:
        movie_vector = model.wv[movie_id]
        user_norm = np.linalg.norm(user_vector)
        movie_norm = np.linalg.norm(movie_vector)

        if user_norm == 0 or movie_norm == 0:
            similarity = 0
        else:
            similarity = np.dot(user_vector, movie_vector) / (user_norm * movie_norm)

        similarity_scores.append((movie_id, similarity))

    similarity_scores = sorted(similarity_scores, key=lambda x: x[1], reverse=True)
    top_recommendations = similarity_scores[:top_n]

    recommended_movies = []
    for movie_id, score in top_recommendations:
        movie_data = metadata[metadata['movieId'] == int(movie_id)].iloc[0]
        recommended_movies.append({
            'movieId': movie_id,
            'title': movie_data['title'],
            'genres': movie_data['genres']
        })

    return recommended_movies



class search_movie(Resource):
    def post(self):
        print("In search movie")
        user_input = request.json
        title = user_input.get('title')
        print("Movie: ", title)
        if not title:
            return {"error": "No title provided"}, 400
        titles = search(title)
        print(titles['movieId'])
        titles_dict = titles.to_dict(orient='records')
        return jsonify(titles_dict)


class similar_recs(Resource):
    def get(self):
        return {"message": "Hello, World! This is a GET request."}, 200


class ProfileRecs(Resource):
    def post(self):
        user_input = request.json
        ratings = user_input.get('ratings', {})

        if not ratings:
            return {"error": "No ratings provided"}, 400

        word2vec_model = get_model_from_s3()

        user_vector = generate_user_profile(ratings, word2vec_model)
        genres_cleaned = get_genres_from_s3()
        recommendations = recommend_movies(user_vector, word2vec_model, genres_cleaned, top_n=10)

        return jsonify({"recommended_movies": recommendations})


class findMovieTitle(Resource):
    def post(self):
        data = request.get_json()
        movie_title = data.get('title')

        # Preprocess the movie title to remove common words like 'the'
        processed_title = re.sub(r'\b(the|a|an)\b', '', movie_title, flags=re.IGNORECASE).strip()

        # Check for exact match
        genres_cleaned = get_genres_from_s3()
        exact_match = genres_cleaned[genres_cleaned['title'].str.lower() == processed_title.lower()]
        if not exact_match.empty:
            result = {
                'title': exact_match['title'].values[0],
                'movieId': int(exact_match['movieId'].values[0])
            }
            return jsonify({'exact_match': result})

        # Find the top 5 matching movie titles
        matches = process.extract(processed_title, genres_cleaned['title'], limit=5)
        top_5_titles = [
            {'title': match[0],
             'movieId': int(genres_cleaned[genres_cleaned['title'] == match[0]]['movieId'].values[0])}
            for match in matches
        ]

        return jsonify({'matches': top_5_titles})


class test_api(Resource):
    def get(self):
        print("In Test API GET")
        return {"message": "Hello, World! This is a GET request."}, 200
    
    def post(self):
        print("In Test API POST")
        data = request.get_json()
        print("Data Received: ", data)
        return {"message": "Hello, World! This is a POST request."}, 200

class test_db(Resource):
    def get(self):
        print("In TestDB")
        response = {}
        items = {}
        try:
            # Connect to the DataBase
            conn = connect()
            # QUERY
            query = """
                SELECT * FROM movies.user_ratings;
                """
            # The query is executed here
            items = execute(query, "get", conn)
            # The return message and result from query execution
            response["message"] = "successful"
            # print(items)
            response["result"] = items["result"]
            # Returns code and response
            return response, 200
        except:
            raise BadRequest(
                "Movies Request failed, please try again later.")
        finally:
            disconnect(conn)


# POST requests
api.add_resource(ProfileRecs, '/api/v2/profile')
api.add_resource(findMovieTitle, '/api/v2/findMovieTitle')
api.add_resource(similar_recs, '/api/v2/similar')
api.add_resource(search_movie, '/api/v2/search')
api.add_resource(test_api, '/api/v2/testAPI')
api.add_resource(test_db, '/api/v2/testDB')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4070)

