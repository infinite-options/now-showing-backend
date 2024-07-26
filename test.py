import os
import pandas as pd
import boto3
import numpy as np
from gensim.models import Word2Vec
from io import StringIO
from flask_restful import Resource, Api
from flask import request, jsonify, Flask


app = Flask(__name__)
api = Api(app)


# Function to load Word2Vec embeddings
def load_word2vec_embeddings(file_path):
    model = Word2Vec.load(file_path)
    movie_embeddings = {int(movie): model.wv[movie] for movie in model.wv.index_to_key}
    return movie_embeddings


def getGenresFromS3():
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


class ProfileRecs(Resource):
    def post(self):
        data = dict(request.json)
        user_ratings = {int(k): float(v) for k, v in data['ratings'].items()}

        # Load Word2Vec embeddings
        movie_embeddings = load_word2vec_embeddings('word2vec_movie_ratings_embeddings.model')

        genres_cleaned = getGenresFromS3()

        # Create the new user embedding
        num_features = next(iter(movie_embeddings.values())).shape[0]
        new_user_vector = np.zeros(num_features)
        total_ratings = 0

        for movie_id, rating in user_ratings.items():
            if movie_id in movie_embeddings:
                new_user_vector += rating * movie_embeddings[movie_id]
                total_ratings += rating

        if total_ratings > 0:
            new_user_vector /= total_ratings

        # Compute cosine similarity
        all_movie_vectors = np.array(list(movie_embeddings.values()))
        cosine_similarities = np.dot(all_movie_vectors, new_user_vector) / (
                np.linalg.norm(all_movie_vectors, axis=1) * np.linalg.norm(new_user_vector))
        top_indices = cosine_similarities.argsort()[-5:][::-1]

        # Recommend top 5 movies
        movie_ids = list(movie_embeddings.keys())
        recommended_movie_ids = [movie_ids[i] for i in top_indices]
        recommended_movie_details = genres_cleaned[genres_cleaned['movieId'].isin(recommended_movie_ids)]

        recommendations = recommended_movie_details[['movieId', 'title', 'genres']]

        try:
            return jsonify(recommendations.to_dict(orient='records'))
        except Exception as e:
            return jsonify({"error": str(e)}), 500


# POST requests
api.add_resource(ProfileRecs, '/api/v2/profile')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)
