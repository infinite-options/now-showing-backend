import pandas as pd
from gensim.models import Word2Vec
from flask_restful import Api
from flask import Flask

app = Flask(__name__)
api = Api(app)


# def build_user_movie_sequences(ratings_df):
#     user_movie_dict = {}
#     for user, group in ratings_df.groupby('userId'):
#         user_movie_dict[user] = list(group['movieId'])
#     return list(user_movie_dict.values())

def build_user_movie_sequences_with_ratings(ratings_df):
    user_movie_dict = {}
    for user, group in ratings_df.groupby('userId'):
        user_movie_dict[user] = []
        for movie_id, rating in zip(group['movieId'], group['rating']):
            user_movie_dict[user].extend([movie_id] * int(rating))  # Repeat movie_id based on rating
    return list(user_movie_dict.values())


def train_word2vec_model(sequences, vector_size=100, window=5, min_count=1, workers=4, epochs=10):
    model = Word2Vec(sentences=sequences, vector_size=vector_size, window=window, min_count=min_count, workers=workers)
    model.train(sequences, total_examples=len(sequences), epochs=epochs)
    return model


# Get ratings and build user-movie sequences
ratings_cleaned = pd.read_csv("/Users/anisha/Desktop/Infinite Options/Rec Sys/dataset/ratings.csv")
user_movie_sequences = build_user_movie_sequences_with_ratings(ratings_cleaned)

# Train Word2Vec model
word2vec_model = train_word2vec_model(user_movie_sequences)

# Save the Word2Vec model
word2vec_model.save('word2vec_movie_ratings_embeddings.model')





