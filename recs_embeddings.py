import os
import boto3
import numpy as np
import pandas as pd
from io import StringIO
from gensim.models import Word2Vec
from flask import Flask, request, jsonify
from flask_restful import Api, Resource

# Load the pre-trained Word2Vec model
word2vec_model = Word2Vec.load('word2vec_movie_ratings_embeddings.model')

app = Flask(__name__)
api = Api(app)


def load_model_from_s3():
    s3_access_key = os.getenv('MW_KEY')
    s3_secret_key = os.getenv('MW_SECRET')
    s3_bucket_name = os.getenv('BUCKET_NAME')
    s3_file_key_word2vec_model = os.getenv('S3_PATH_KEY_WORD2VEC_MODEL')

    s3_client = boto3.client('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)
    print("after s3 connect")

    local_model_path = '/tmp/word2vec_movie_ratings_embeddings.model'
    s3_client.download_file(s3_bucket_name, s3_file_key_word2vec_model, local_model_path)
    print("model downloaded from s3")

    model = Word2Vec.load(local_model_path)
    return model


def get_genres_from_s3():
    s3_access_key = os.getenv('MW_KEY')
    s3_secret_key = os.getenv('MW_SECRET')
    s3_bucket_name = os.getenv('BUCKET_NAME')
    s3_file_key_genres = os.getenv('S3_PATH_KEY_GENRES')

    s3_client = boto3.client('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)
    print("after s3 connect")

    genres_response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_key_genres)
    print("the response of the S3", genres_response)
    print()

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


class ProfileRecs(Resource):
    def post(self):
        user_input = request.json
        ratings = user_input.get('ratings', {})

        if not ratings:
            return {"error": "No ratings provided"}, 400

        user_vector = generate_user_profile(ratings, word2vec_model)
        genres_cleaned = get_genres_from_s3()
        recommendations = recommend_movies(user_vector, word2vec_model, genres_cleaned, top_n=10)

        return jsonify({"recommended_movies": recommendations})


# POST requests
api.add_resource(ProfileRecs, '/api/v2/profile')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)

