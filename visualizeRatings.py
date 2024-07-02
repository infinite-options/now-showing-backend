from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
from typing import Dict
import boto3
from fuzzywuzzy import process
from typing import List

client = boto3.client("s3")
app = FastAPI()

# ratings = pd.read_csv("https://now-showing.s3.us-west-1.amazonaws.com/movies_ratings.csv")
# movies = pd.read_csv("https://now-showing.s3.us-west-1.amazonaws.com/movies_genres.csv")

# ratings = pd.read_csv("/Users/anisha/Desktop/Infinite Options/Rec Sys/dataset/ratings.csv")
# movies = pd.read_csv("/Users/anisha/Desktop/Infinite Options/Rec Sys/dataset/movies.csv")

movies = pd.read_csv('s3://now-showing/movies_genres.csv')
ratings = pd.read_csv('s3://now-showing/movies_ratings.csv')


# Preprocess the dataset
# Remove movies that do not have any genres listed from both movies and ratings dataset
movies = movies[movies['genres'] != '(no genres listed)']
unique_movie_ids = movies['movieId'].unique()
ratings = ratings[ratings['movieId'].isin(unique_movie_ids)]

# Remove users from the ratings dataset who have rated <= 200 movies
above_threshold_users = ratings['userId'].value_counts() > 200
above_threshold_user_indices = above_threshold_users[above_threshold_users].index
ratings = ratings[ratings['userId'].isin(above_threshold_user_indices)]

# merge the ratings and movies dataset
ratings_with_movies = ratings.merge(movies, on='movieId')
# Count the number of ratings received by each movie
num_rating = ratings_with_movies.groupby('title')['rating'].count().reset_index()
num_rating.rename(columns={
    'rating': 'num_of_rating'
}, inplace=True)

# Remove the movies that have less than 50 ratings
num_rating = num_rating[num_rating['num_of_rating'] >= 50]
ratings_with_movies = ratings_with_movies.merge(num_rating, on='title')
ratings_with_movies.drop_duplicates(['userId', 'movieId'], inplace=True)

# Find the unique movieIds in the merged dataset
unique_movie_ids = ratings_with_movies['movieId'].unique()

# Keep only unique_movie_ids in the movies and ratings dataset
movies = movies[movies['movieId'].isin(unique_movie_ids)]
ratings = ratings[ratings['movieId'].isin(unique_movie_ids)]

# Sort the datasets by movieId
ratings.sort_values('movieId')
movies.sort_values('movieId')
ratings_with_movies.sort_values('movieId')

# Collaborative filtering using rating
user_item_matrix = ratings.pivot_table(columns='userId', index='movieId', values='rating').fillna(0)


class MovieRequest(BaseModel):
    title: str


class MovieResponse(BaseModel):
    similar_titles: List[str]


class UserRatings(BaseModel):
    ratings: Dict[int, float]


@app.post("/find_movie/", response_model=MovieResponse)
def find_movie(movie_request: MovieRequest):
    title = movie_request.title
    # Check if the exact title exists
    exact_match = movies[movies['title'].str.lower() == title.lower()]
    if not exact_match.empty:
        return MovieResponse(similar_titles=[title])

    # If exact match is not found, find similar titles
    similar_titles = process.extract(title, movies['title'], limit=5)
    similar_titles_list = [title for title, score, idx in similar_titles]

    return MovieResponse(similar_titles=similar_titles_list)


@app.post("/recommend")
def recommend(user_ratings: UserRatings):
    if not user_ratings.ratings:
        raise HTTPException(status_code=400, detail="No input provided")

    # Create a new user with these ratings
    user_item_matrix['new_user'] = 0
    for movie_id, rating in user_ratings.ratings.items():
        if movie_id in user_item_matrix.index:
            user_item_matrix.at[movie_id, 'new_user'] = rating

    # Fit the Nearest Neighbors model
    model = NearestNeighbors(metric='cosine', algorithm='brute')
    model.fit(user_item_matrix.T)

    # Find the nearest neighbors for the new user
    distances, indices = model.kneighbors([user_item_matrix.T.loc['new_user']], n_neighbors=5)

    # Get the indices of similar users
    similar_user_ids = user_item_matrix.columns[indices.flatten()]
    similar_user_ids = similar_user_ids.drop('new_user')

    # Aggregate ratings of similar users for recommendation
    similar_users_ratings = user_item_matrix[similar_user_ids].mean(axis=1)

    # Filter out movies already rated by the new user
    unseen_movies_ratings = similar_users_ratings.drop(index=user_ratings.ratings.keys())

    # Recommend top 10 movies
    recommended_movies = unseen_movies_ratings.sort_values(ascending=False).head(10)
    recommended_movie_ids = recommended_movies.index
    recommended_movie_details = movies[movies['movieId'].isin(recommended_movie_ids)]

    # Calculate the average rating for each movie
    average_ratings = ratings.groupby('movieId')['rating'].mean()

    recommendations = []
    for movie_id in recommended_movie_ids:
        movie_details = recommended_movie_details[recommended_movie_details['movieId'] == movie_id].iloc[0]
        avg_rating = average_ratings[movie_id] if movie_id in average_ratings else np.nan
        recommendations.append({
            'movieId': int(movie_id),
            'title': movie_details['title'],
            'genres': movie_details['genres'],
            'average_rating': avg_rating
        })

    return recommendations


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=8000)