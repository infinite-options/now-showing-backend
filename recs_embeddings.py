import os
import boto3
import numpy as np
import pandas as pd
from io import StringIO
from gensim.models import Word2Vec
from flask import Flask, request, jsonify
from flask_restful import Api, Resource
from fuzzywuzzy import process
import requests
import re
import mysql.connector
from config import Config


app = Flask(__name__)
api = Api(app)
app.config.from_object(Config)


# Database connection
print("Connecting to Database")
def get_db_connection():
    conn = mysql.connector.connect(
        host=app.config['MYSQL_HOST'],
        port=int(app.config['MYSQL_PORT']),
        user=app.config['MYSQL_USER'],
        password=app.config['MYSQL_PASSWORD'],
        database=app.config['MYSQL_DB']
    )
    return conn


class AddMovieRating(Resource):
    print("Using AddMovieRating")
    def post(self):
        print("Using AddMovieRating POST")
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


def generate_user_profile(ratings, model, genres_cleaned):
    user_vector = np.zeros(model.vector_size)
    total_weight = 0.0

    for title, rating in ratings.items():
        movie_id = genres_cleaned[genres_cleaned['title'] == title]['movieId'].values[0]
        if int(movie_id) in model.wv:
            user_vector += model.wv[int(movie_id)] * rating
            total_weight += rating

    if total_weight > 0:
        user_vector /= total_weight

    return user_vector


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


def fetch_tmdb_data(movie_title):
    api_key = os.getenv('TMDB_API_KEY')
    search_url = f"https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={movie_title}"
    response = requests.get(search_url)
    search_data = response.json()

    if search_data['results']:
        movie_id = search_data['results'][0]['id']
        movie_url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&language=en-US"
        credits_url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={api_key}&language=en-US"

        movie_response = requests.get(movie_url)
        credits_response = requests.get(credits_url)

        movie_data = movie_response.json()
        credits_data = credits_response.json()

        return {
            "tmdb_id": movie_id,
            "poster": f"https://image.tmdb.org/t/p/w500{movie_data.get('poster_path')}",
            "overview": movie_data.get('overview', 'Overview not available'),
            "rating": f"{movie_data.get('vote_average', 'Not rated')}/10 ({movie_data.get('vote_count', 0)} votes)",
            "cast": ", ".join([actor['name'] for actor in credits_data.get('cast', [])[:5]])
        }
    return None


class ProfileRecs(Resource):
    print("In ProfileRecs")
    def post(self):
        print("In ProfileRecs POST")
        user_input = request.json
        ratings = user_input.get('ratings', {})

        if not ratings:
            return {"error": "No ratings provided"}, 400

        word2vec_model = get_model_from_s3()
        genres_cleaned = get_genres_from_s3()

        # Generate user profile using titles
        user_vector = generate_user_profile(ratings, word2vec_model, genres_cleaned)
        recommendations = recommend_movies(user_vector, word2vec_model, genres_cleaned, top_n=10)

        # Add TMDb data to recommendations
        detailed_recommendations = []
        for recommendation in recommendations:
            movie_title = recommendation['title']
            tmdb_info = fetch_tmdb_data(movie_title)

            if tmdb_info:
                recommendation.update(tmdb_info)
            detailed_recommendations.append(recommendation)

        return jsonify({"recommended_movies": detailed_recommendations})


class FindMovieTitle(Resource):
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


# POST requests
api.add_resource(ProfileRecs, '/api/v2/profile')
api.add_resource(FindMovieTitle, '/api/v2/findMovieTitle')
api.add_resource(AddMovieRating, '/api/v2/add_movie_rating')
# api.add_resource(AddMovieRating, '/api/v2/test')
api.add_resource(test_api, '/api/v2/test')
# api.add_resource(test_api, '/api/v2/similar')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)

