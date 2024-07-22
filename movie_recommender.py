import dask.dataframe as dd
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import boto3
from flask import Flask, request, jsonify
import requests
from fuzzywuzzy import process

app = Flask(__name__)

client = boto3.client("s3")

# Function to read Parquet files in chunks


def read_parquet_in_chunks(file_path, chunksize=100000):
    dataset = pq.ParquetDataset(file_path)
    table = dataset.read()
    num_rows = table.num_rows

    for i in range(0, num_rows, chunksize):
        chunk = table.slice(i, chunksize).to_pandas()
        yield chunk


# Read Parquet files in chunks
movies = pd.concat(read_parquet_in_chunks('s3://now-showing/genres.parquet'))
ratings = pd.concat(read_parquet_in_chunks('s3://now-showing/ratings.parquet'))

# TMDB API key
API_KEY = "b8b76ccfa61c6e85ca7e096d905a7d63"

# Global variables
movie_id = 1  # default movie choice
rec_num = 10


def fetch_tmdb_data(movie_title):
    search_url = f"https://api.themoviedb.org/3/search/movie?api_key={API_KEY}&query={movie_title}"
    response = requests.get(search_url)
    search_data = response.json()

    if search_data['results']:
        movie_id = search_data['results'][0]['id']
        movie_url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}&language=en-US"
        credits_url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={API_KEY}&language=en-US"

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


def find_similar_movies(movie_id):
    similar_users = ratings[(ratings["movieId"] == movie_id) & (
        ratings["rating"] > 4)]["userId"].unique()
    similar_user_recs = ratings[(ratings["userId"].isin(
        similar_users)) & (ratings["rating"] > 4)]["movieId"]

    similar_user_recs = similar_user_recs.value_counts() / len(similar_users)
    similar_user_recs = similar_user_recs[similar_user_recs > .1]

    all_users = ratings[(ratings["movieId"].isin(
        similar_user_recs.index)) & (ratings["rating"] > 4)]
    all_user_recs = all_users["movieId"].value_counts(
    ) / len(all_users["userId"].unique())

    rec_percentages = pd.concat([similar_user_recs, all_user_recs], axis=1)
    rec_percentages.columns = ["similar", "all"]

    rec_percentages["score"] = rec_percentages["similar"] / \
        rec_percentages["all"]
    rec_percentages = rec_percentages.sort_values("score", ascending=False)

    return rec_percentages.head(rec_num).merge(movies, left_index=True, right_on="movieId")

# ENDPOINTS


@app.route('/recommend', methods=['POST'])
def get_recommendations():
    data = request.json
    movie_id = data.get('movie_id')
    if not movie_id:
        return jsonify({"error": "No movie ID provided"}), 400

    try:
        recommendations = find_similar_movies(movie_id)

        # Fetch TMDB data for each recommended movie
        for _, row in recommendations.iterrows():
            tmdb_data = fetch_tmdb_data(row['title'])
            if tmdb_data:
                recommendations.loc[recommendations['title'] ==
                                    row['title'], 'tmdb_data'] = str(tmdb_data)

        return jsonify(recommendations.to_dict(orient='records'))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/movies', methods=['GET'])
def get_movies_list():
    return jsonify(movies[['movieId', 'title']].to_dict(orient='records'))


@app.route('/recommend_by_name', methods=['POST'])
def get_recommendations_by_name():
    data = request.json
    movie_name = data.get('movie_name')
    if not movie_name:
        return jsonify({"error": "No movie name provided"}), 400

    try:
        # Find matching movies
        matching_movies = movies[movies['title'].str.contains(
            movie_name, case=False, na=False)]

        if matching_movies.empty:
            return jsonify({"error": f"No movies found matching '{movie_name}'"}), 404

        # If multiple matches, use the first one
        matched_movie = matching_movies.iloc[0]
        movie_id = matched_movie['movieId']

        recommendations = find_similar_movies(movie_id)

        # Fetch TMDB data for each recommended movie
        for _, row in recommendations.iterrows():
            tmdb_data = fetch_tmdb_data(row['title'])
            if tmdb_data:
                recommendations.loc[recommendations['title'] ==
                                    row['title'], 'tmdb_data'] = str(tmdb_data)

        return jsonify({
            "matched_movies": matching_movies[['movieId', 'title']].to_dict(orient='records'),
            "used_for_recommendations": matched_movie.to_dict(),
            "recommendations": recommendations.to_dict(orient='records')
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/movie/<int:movie_id>', methods=['GET'])
def get_movie_details(movie_id):
    movie = movies[movies['movieId'] == movie_id]
    if movie.empty:
        return jsonify({"error": "Movie not found"}), 404

    movie_title = movie['title'].iloc[0]
    tmdb_data = fetch_tmdb_data(movie_title)

    if tmdb_data:
        return jsonify({**movie.to_dict(orient='records')[0], **tmdb_data})
    else:
        return jsonify(movie.to_dict(orient='records')[0])


if __name__ == '__main__':
    app.run(debug=True)
