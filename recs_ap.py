import pandas as pd
import boto3
from flask import Flask, request, jsonify
from sklearn.neighbors import NearestNeighbors

app = Flask(__name__)

client = boto3.client("s3")

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


@app.route('/recommend', methods=['POST'])
def recommend():
    data = dict(request.json)
    # Read and cast the movie ratings dictionary
    user_ratings = {int(k): float(v) for k, v in data['ratings'].items()}
    # Collaborative filtering using rating
    user_item_matrix = ratings.pivot_table(columns='userId', index='movieId', values='rating').fillna(0)

    print("User ratings: ")
    print(user_ratings)
    # Create a new user with these ratings
    user_item_matrix['new_user'] = 0
    for movie_id, rating in user_ratings.items():
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
    unseen_movies_ratings = similar_users_ratings.drop(index=user_ratings.keys())

    # Recommend top 10 movies
    recommended_movies = unseen_movies_ratings.sort_values(ascending=False).head(10)
    recommended_movie_ids = recommended_movies.index
    recommended_movie_details = movies[movies['movieId'].isin(recommended_movie_ids)]

    # Calculate the average rating for each movie
    average_ratings = ratings.groupby('movieId')['rating'].mean()

    recommended_movie_details = recommended_movie_details.merge(average_ratings, on='movieId')
    recommended_movie_details.rename(columns={
        'rating': 'avg_rating'
    }, inplace=True)
    recommendations = recommended_movie_details[['movieId', 'title', 'genres', 'avg_rating']]

    try:
        return jsonify(recommendations.to_dict(orient='records'))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)