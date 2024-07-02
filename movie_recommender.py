import pandas as pd
import re
import numpy as np
import tkinter as tk
from tkinter import ttk, messagebox
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

rec_num = 100
user_weight = 0.5
genre_weight = 0.5
movies = pd.read_csv("movies.csv")
ratings = pd.read_csv("ratings.csv")
movie_id = 1  # default movie choice

# Create a count vectorizer for genres
count_vectorizer = CountVectorizer()
genre_matrix = count_vectorizer.fit_transform(movies['genres'])

# Compute cosine similarity for genres
genre_similarity = cosine_similarity(genre_matrix)

# Convert to a DataFrame for easier indexing
genre_similarity_df = pd.DataFrame(
    genre_similarity, index=movies['movieId'], columns=movies['movieId'])


def search(title):
    matching_movies = movies[movies['title'].str.contains(
        title, case=False, na=False)]
    return matching_movies[['movieId', 'title', 'genres']]  # return results


def find_similar_movies(movie_id):
    # find users who like the same movies and rated the movie highly
    similar_users = ratings[(ratings["movieId"] == movie_id) & (
        ratings["rating"] > 4)]["userId"].unique()
    similar_user_recs = ratings[(ratings["userId"].isin(
        similar_users)) & (ratings["rating"] > 4)]["movieId"]

    # look for 10% or more of users that are similar
    similar_user_recs = similar_user_recs.value_counts() / len(similar_users)
    similar_user_recs = similar_user_recs[similar_user_recs > .1]

    all_users = ratings[(ratings["movieId"].isin(similar_user_recs.index)) & (
        ratings["rating"] > 4)]  # all users who've watched movies that were recommended to us
    # find percentage of all users who recommended the movies
    all_user_recs = all_users["movieId"].value_counts(
    ) / len(all_users["userId"].unique())

    # concatinate similar user and all user ratings
    rec_percentages = pd.concat([similar_user_recs, all_user_recs], axis=1)
    rec_percentages.columns = ["similar", "all"]

    rec_percentages["score"] = rec_percentages["similar"] / \
        rec_percentages["all"]  # generate score

    rec_percentages = rec_percentages.sort_values(
        "score", ascending=False)  # sort score

    return rec_percentages.head(rec_num).merge(movies, left_index=True, right_on="movieId")


def recommend_genre(movie_id):
    # Validate weights
    if user_weight + genre_weight != 1:
        raise ValueError("user_weight and genre_weight must sum to 1.")

    # Get genre-based recommendations
    genre_similarities = genre_similarity_df[movie_id]
    genre_recommendations = genre_similarities.sort_values(ascending=False)

    # Get user-based recommendations using the provided function
    user_recommendations = find_similar_movies(movie_id)

    # Merge genre and user recommendations
    genre_recommendations = genre_recommendations.reset_index()
    genre_recommendations.columns = ['movieId', 'genre_similarity']

    combined_recommendations = pd.merge(
        user_recommendations, genre_recommendations, on='movieId', how='outer')

    # Normalize scores
    combined_recommendations['user_score'] = combined_recommendations['score'] / \
        combined_recommendations['score'].max()
    combined_recommendations['genre_score'] = combined_recommendations['genre_similarity'] / \
        combined_recommendations['genre_similarity'].max()

    # Combine scores
    combined_recommendations['combined_score'] = (
        user_weight * combined_recommendations['user_score']) + (genre_weight * combined_recommendations['genre_score'])

    # Sort by the combined score
    combined_recommendations = combined_recommendations.sort_values(
        'combined_score', ascending=False).head(rec_num)

    # Select relevant columns to display
    combined_recommendations = combined_recommendations[[
        'movieId', 'title', 'genres', 'user_score', 'genre_score', 'combined_score']]

    return combined_recommendations

# Tkinter GUI for searching movies and displaying recommendations


# def search_movie():
#     movie_title = search_var.get()
#     if not movie_title:
#         messagebox.showwarning("Input Error", "Please enter a movie title.")
#         return

#     movie = movies[movies['title'].str.contains(movie_title, case=False)]
#     if movie.empty:
#         messagebox.showwarning("No Results", "No movie found with that title.")
#         return

#     movie_id = movie.iloc[0]['movieId']
#     recommendations = recommend_genre(movie_id)

#     for i in tree.get_children():
#         tree.delete(i)

#     for _, row in recommendations.iterrows():
#         tree.insert("", "end", values=(
#             row['movieId'], row['title'], row['user_score'], row['genre_score'], row['combined_score']))


# results = search("Lost In Translation")
# movie_id = results.iloc[0]["movieId"]
# similar_movies = find_similar_movies(movie_id)
# similar_movies_genre = recommend_genre(movie_id)
# Initialize Tkinter
# root = tk.Tk()
# root.title("Movie Recommender")

# search_var = tk.StringVar()
# tk.Label(root, text="Search Movie:").grid(row=0, column=0, padx=10, pady=10)
# tk.Entry(root, textvariable=search_var).grid(row=0, column=1, padx=10, pady=10)
# tk.Button(root, text="Search", command=search_movie).grid(
#     row=0, column=2, padx=10, pady=10)

# columns = ("movieId", "title", "combined_score", "user_score", "genre_score")
# tree = ttk.Treeview(root, columns=columns, show='headings')
# for col in columns:
#     tree.heading(col, text=col)
# tree.grid(row=1, column=0, columnspan=3, padx=10, pady=10)

# root.mainloop()
