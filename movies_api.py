import json
from flask_restful import Resource, Api
import mysql.connector
from flask import Flask, request, jsonify
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import io
import gzip
import requests
import requests

cnx = mysql.connector.connect(user='admin', password='prashant',
                              host='io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com',
                              database='movies')

app = Flask(__name__)
app.config['DEBUG'] = True
api = Api(app)

class search(Resource):
    def get(self):
        movie = request.json.get('movie')
        movie.replace(" ", "+")
        try:
            response = requests.get(
                "https://api.themoviedb.org/3/search/movie?api_key=916893c3e71b0b37e324a142b99253f0&query={}".format(movie))
        except:
            print("Issue with TMDB Movie Search API")
        response_data = response.json()
        if len(response_data['results']) == 1:
            id = response_data['results'][0]['id']
            try:
                response = requests.get(
                    "https://api.themoviedb.org/3/movie/{}?api_key=916893c3e71b0b37e324a142b99253f0".format(id))
            except:
                print("Issue with TMDB Movie_ID Search API")
            response_data = response.json()
            movie_id = response_data['id']
            # with open('movie_details.txt', 'w', encoding='utf-8') as f:
            #     f.write("{}: {}".format(movie_id, response_data['original_title']))
            return 'Updated Records in Local Machine for Movie: {}, Movie_ID: {}'.format(
                response_data['original_title'], movie_id)
            # return movie input the user gave return unique movie id found also return movie id in DB
        elif len(response_data['results']) == 0:
            return 'no such movies found in the database'
        else:
            res = []
            for j in range(len(response_data['results'])):
                response_data['results'][j]['TMDB_ID'] = response_data['results'][j].pop('id')
            for i in range(len(response_data['results'])):
                res.append(dict((k, response_data['results'][i][k]) for k in
                                ['original_title', 'release_date', 'overview', 'TMDB_ID', 'backdrop_path']
                                if k in response_data['results'][i]))
            return res

class search_imdb(Resource):
    def get(self):
        movie_list = []
        url1 = "https://datasets.imdbws.com/title.basics.tsv.gz"
        url2 = "https://datasets.imdbws.com/title.ratings.tsv.gz"

        response1 = requests.get(url1)
        response2 = requests.get(url2)
        content1 = gzip.decompress(response1.content)
        content2 = gzip.decompress(response2.content)
        title_basics = pd.read_csv(io.BytesIO(content1), sep='\t',
                                   usecols=['tconst', 'primaryTitle', 'startYear', 'genres', 'titleType'], dtype=str)
        # title_basics = pd.read_csv('title.basics.tsv.gz', sep='\t', usecols=['tconst', 'primaryTitle', 'startYear', 'genres'], dtype=str)

        # Load title ratings data
        title_ratings = pd.read_csv(io.BytesIO(content2), sep='\t',
                                    dtype={'tconst': str, 'averageRating': float, 'numVotes': int})
        title_basics['startYear'] = title_basics['startYear'].replace('\\N', '0')

        title_basics = title_basics[(title_basics['startYear'].astype(int) > 1985)]
        title_basics = title_basics[(title_basics['titleType'] == 'movie')]
        title_basics['primaryTitle'] = title_basics['primaryTitle'].str.lower()

        title_ratings = title_ratings[(title_ratings['numVotes'] > 50000)]

        df = pd.merge(title_basics, title_ratings, on='tconst')

        df['startYear'] = df['startYear'].replace('\\N', '0')
        df = df[(df['startYear'].astype(int) > 1980) & (df['numVotes'] > 50000)]

        # Fill NA/NaN values in 'genres' column
        df['genres'] = df['genres'].fillna('')

        movie = request.json.get('movie')

        # Get the index of the movie that matches the title
        lst = df[df['primaryTitle'] == movie].index.tolist()
        print("list is: ",lst)
        if len(lst)==0:
            return 'no such movies found in the database'
        elif len(lst)==1:
            print(df.iloc[lst[0]])
            return 'Updated Records in Local Machine for Movie: {}, imdb_ID: {}'.format(df.loc[lst[0],'primaryTitle'],df.loc[lst[0],'tconst'])
        else:
            for idx in lst:
                movie_list.append(df.iloc[idx])
            return movie_list

api.add_resource(search, '/search')
api.add_resource(search_imdb, '/search_imdb')

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=4000)
