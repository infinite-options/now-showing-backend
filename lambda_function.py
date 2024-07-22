import json
import pandas as pd
import boto3
import requests
import json
# from flask import Flask, jsonify

# Initialize S3 client
s3 = boto3.client('s3')

# TMDB API key
API_KEY = "b8b76ccfa61c6e85ca7e096d905a7d63"

# Global variables
rec_num = 10


def load_data_from_s3():
    # Load data from S3
    movies = pd.read_parquet('s3://now-showing/genres.parquet')
    ratings = pd.read_parquet('s3://now-showing/ratings.parquet')
    return movies, ratings


movies, ratings = load_data_from_s3()


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


def find_similar_movies(movie_id, movies, ratings):
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


def get_recommendations():
    data = request.json
    movie_id = data.get('movie_id')
    if not movie_id:
        return {
            'statusCode': 400,
            'body': json.dumps({"error": "No movie ID provided"})
        }

    try:
        recommendations = find_similar_movies(movie_id)

        # Fetch TMDB data for each recommended movie
        for _, row in recommendations.iterrows():
            tmdb_data = fetch_tmdb_data(row['title'])
            if tmdb_data:
                recommendations.loc[recommendations['title'] ==
                                    row['title'], 'tmdb_data'] = str(tmdb_data)

        return {
            'statusCode': 200,
            'body': json.dumps(recommendations.to_dict(orient='records'))
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }


def get_movies_list():
    return {
        'statusCode': 200,
        'body': json.dumps(movies[['movieId', 'title']].to_dict(orient='records'))
    }


def lambda_handler(event, context):
    try:
        http_method = event.get('httpMethod')
        resource = event.get('resource')

        if not http_method or not resource:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid request structure'})
            }

        if resource == '/movies':
            if http_method == 'GET':
                return get_movies_list()
        elif resource == '/movies/{movieId}':
            movie_id = event.get('pathParameters', {}).get('movieId')
            if not movie_id:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Movie ID not provided'})
                }
            if http_method == 'GET':
                return get_movie(movie_id)
        elif resource == '/recommend':
            if http_method == 'POST':
                body = json.loads(event.get('body', '{}'))
                return get_recommendations(body)

        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Unsupported method or resource'})
        }

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
