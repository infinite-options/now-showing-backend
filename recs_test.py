import os
import boto3
import numpy as np
import pandas as pd
from io import StringIO
from gensim.models import Word2Vec
from flask import Flask, request, jsonify
from flask_restful import Api, Resource
from fuzzywuzzy import process
import re


app = Flask(__name__)
api = Api(app)


def get_model_from_s3():
    s3_access_key = os.getenv('MW_KEY')
    s3_secret_key = os.getenv('MW_SECRET')
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
    s3_access_key = os.getenv('MW_KEY')
    s3_secret_key = os.getenv('MW_SECRET')
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


# POST requests
api.add_resource(ProfileRecs, '/api/v2/profile')
api.add_resource(findMovieTitle, '/api/v2/findMovieTitle')
api.add_resource(similar_recs, '/api/v2/similar')
api.add_resource(search_movie, '/api/v2/search')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)

