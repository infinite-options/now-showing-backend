# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

#from flask import Flask, request, render_template, url_for, redirect
#from flask_restful import Resource, Api
import json
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


@app.route('/insert', methods=['GET', 'POST'])
def insert():
    mycursor = cnx.cursor()
    email = request.json.get('email')
    fname = request.json.get('fname')
    lname = request.json.get('lname')
    mycursor.execute("call new_user_id(@record);")
    mycursor.execute("INSERT INTO `Users` (`User_ID`, `Email`, `First_Name`, `Last_Name`) VALUES (@record, %s, %s, %s)",(email, fname, lname))
    cnx.commit()
    cnx.close()
    return 'OK'

@app.route('/display', methods=['GET', 'POST'])
def display():
    mycursor = cnx.cursor(dictionary= True)
    uid = request.json.get('uid')
    mycursor.execute("SELECT * FROM Users WHERE First_Name = '{}'".format(uid))
    data = mycursor.fetchall()
    return json.dumps(data)

@app.route('/rateone', methods=['GET', 'POST'])
def rateone():
    genre = []
    movie = request.json.get('movie')
    movie.replace(" ", "+")
    response = requests.get("https://api.themoviedb.org/3/search/movie?api_key=916893c3e71b0b37e324a142b99253f0&query={}".format(movie))
    response_data = response.json()
    # cpy = response_data
    # for x in cpy['results']:
    #     if movie.lower() not in x['original_title'].lower():
    #         response_data['results'].remove(x)
    if len(response_data['results'])==1:
        id = response_data['results'][0]['id']
        response = requests.get("https://api.themoviedb.org/3/movie/{}?api_key=916893c3e71b0b37e324a142b99253f0".format(id))
        response_data = response.json()
        backdrop_path = response_data['backdrop_path']
        for i in range(len(response_data['genres'])):
            genre.append(response_data['genres'][i]['name'])
        genres = ', '.join(genre)
        id = response_data['id']
        mycursor = cnx.cursor()
        user_id = request.json.get('user_id')
        user_rating = request.json.get('user_rating')
        mycursor.execute("call new_rating_id(@record);")
        mycursor.execute("INSERT INTO `Ratings`(`Rating_ID`, `User_ID`, `Movie_Name`, `User_Rating`, `Genres`, `Backdrop`, `Movie_ID`) VALUES(@record, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE User_Rating = %s;",(user_id, response_data['original_title'], user_rating, genres, backdrop_path, id, user_rating))
        cnx.commit()
        cnx.close()
        return 'ok'
    elif len(response_data['results'])==0:
        return 'no such movies found in the database'
    else:
        res = []
        for i in range(len(response_data['results'])):
            res.append(dict((k, response_data['results'][i][k]) for k in ['original_title', 'release_date','overview','id']
                       if k in response_data['results'][i]))
        return res

@app.route('/ratetwo', methods=['GET', 'POST'])
def ratetwo():
    genre = []
    id = request.json.get('id')
    response = requests.get("https://api.themoviedb.org/3/movie/{}?api_key=916893c3e71b0b37e324a142b99253f0".format(id))
    response_data = response.json()
    backdrop_path = response_data['backdrop_path']
    for i in range(len(response_data['genres'])):
        genre.append(response_data['genres'][i]['name'])
    genres = ', '.join(genre)
    id = response_data['id']
    user_id = request.json.get('user_id')
    user_rating = request.json.get('user_rating')
    mycursor = cnx.cursor()
    mycursor.execute("call new_rating_id(@record);")
    #mycursor.execute("INSERT INTO `Ratings` (`Rating_ID`, `User_ID`, `Movie_Name`, `User_Rating`) VALUES (@record, %s, %s, %s)",(user_id, response_data['results'][0]['original_title'], user_rating))
    mycursor.execute("INSERT INTO `Ratings`(`Rating_ID`, `User_ID`, `Movie_Name`, `User_Rating`, `Genres`, `Backdrop`, `Movie_ID`) VALUES(@record, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE User_Rating = %s;",(user_id, response_data['original_title'], user_rating, genres, backdrop_path, id, user_rating))
    cnx.commit()
    cnx.close()
    return 'ok'

@app.route('/ratelist', methods=['GET', 'POST'])
def ratelist():
    user_rating = []
    movies = []
    genre = []
    user_id = '100-000001'
    movie_list = request.json.get('movies')
    for i in range(len(movie_list)):
        user_rating.append(movie_list[i]['rating'])
        movies.append(movie_list[i]['movie_title'])
    print(len(movies))
    print(len(user_rating))
    for i in range(len(movies)):
        response = (requests.get("https://api.themoviedb.org/3/search/movie?api_key=916893c3e71b0b37e324a142b99253f0&query={}".format(movies[i])))
        response_data = response.json()
        resp = requests.get("https://api.themoviedb.org/3/movie/{}?api_key=916893c3e71b0b37e324a142b99253f0".format(response_data['results'][0]['id']))
        response_data = resp.json()
        backdrop_path = response_data['backdrop_path']
        for j in range(len(response_data['genres'])):
            genre.append(response_data['genres'][j]['name'])
        genres = ', '.join(genre)
        id = response_data['id']
        mycursor = cnx.cursor()
        mycursor.execute("call new_rating_id(@record);")
        # mycursor.execute("INSERT INTO `Ratings` (`Rating_ID`, `User_ID`, `Movie_Name`, `User_Rating`) VALUES (@record, %s, %s, %s)",(user_id, response_data['results'][0]['original_title'], user_rating))
        mycursor.execute("INSERT INTO `Ratings`(`Rating_ID`, `User_ID`, `Movie_Name`, `User_Rating`, `Genres`, `Backdrop`, `Movie_ID`) VALUES(@record, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE User_Rating = %s;",(user_id, response_data['original_title'], user_rating[i], genres, backdrop_path, id, user_rating[i]))
        cnx.commit()
    cnx.close()
    return 'ok'

@app.route('/search', methods=['GET', 'POST'])
def search():
    movie = request.json.get('movie')
    movie.replace(" ", "+")
    response = requests.get("https://api.themoviedb.org/3/search/movie?api_key=916893c3e71b0b37e324a142b99253f0&query={}".format(movie))
    response_data = response.json()
    if len(response_data['results'])==1:
        id = response_data['results'][0]['id']
        response = requests.get("https://api.themoviedb.org/3/movie/{}?api_key=916893c3e71b0b37e324a142b99253f0".format(id))
        response_data = response.json()
        movie_id = response_data['id']
        with open('movie_details.txt', 'w', encoding='utf-8') as f:
            f.write("{}: {}".format(movie_id, response_data['original_title']))
        return 'Updated Records in Local Machine for Movie: {}, Movie_ID: {}'.format(response_data['original_title'], movie_id)
        #return movie input the user gave return unique movie id found also return movie id in DB
    elif len(response_data['results'])==0:
        return 'no such movies found in the database'
    else:
        res = []
        for j in range(len(response_data['results'])):
            response_data['results'][j]['TMDB_ID'] = response_data['results'][j].pop('id')
        for i in range(len(response_data['results'])):
            res.append(dict((k, response_data['results'][i][k]) for k in ['original_title', 'release_date','overview','TMDB_ID','backdrop_path']
                       if k in response_data['results'][i]))
        return res

@app.route('/recommend', methods=['GET', 'POST'])
def recommend():
    url1 = "https://datasets.imdbws.com/title.basics.tsv.gz"
    url2 = "https://datasets.imdbws.com/title.ratings.tsv.gz"

    user_movies_list = request.get_json()
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

    # Vectorize the genres - convert the text to a matrix of token counts
    count_vect = CountVectorizer(stop_words='english')
    count_matrix = count_vect.fit_transform(df['genres'])

    # Compute the Cosine Similarity matrix based on the count_matrix
    cosine_sim = cosine_similarity(count_matrix, count_matrix)

    # Reset index of df and construct reverse mapping
    indices = pd.Series(df.index, index=df['primaryTitle']).drop_duplicates()
    # print(indices)

    recommended_movies = []

    for movie_rating_dict in user_movies_list:
        user_movie = movie_rating_dict['title']

        # Get the index of the movie that matches the title
        idx = indices[user_movie]

        # Get the pairwsie similarity scores of all movies with that movie
        sim_scores = list(enumerate(cosine_sim[idx]))

        # Sort the movies based on the similarity scores
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

        # Get the scores of the 5 most similar movies
        sim_scores = sim_scores[1:6]

        # Get the movie indices
        movie_indices = [i[0] for i in sim_scores]

        # Get the top 5 most similar movies
        recommended_movies.extend(df['primaryTitle'].iloc[movie_indices].tolist())

    # Remove duplicates and movies already seen by the user
    recommended_movies = list(set(recommended_movies) - set([d['title'] for d in user_movies_list]))

    # Return just the titles of the recommended movies
    recommended_movies = [{"title": title} for title in recommended_movies[:5]]
    return recommended_movies

if __name__ == "__main__":
    app.run(debug = True)


#Finding all ratings given by a user for all movies
#mycursor.execute("SELECT Ratings.* FROM Ratings INNER JOIN Users ON Users.User_ID=Ratings.User_ID WHERE Ratings.User_ID='U1'")

#mycursor.execute("call new_user_id(@record);")
#mycursor.execute("INSERT INTO `Users` (`User_ID`, `Email`, `First_Name`, `Last_Name`) VALUES (@record, 'Lance321@gmail.com', 'Tom', 'Lance');")

#Inserting into databases
#mycursor.execute("INSERT INTO `Users` (`User_ID`, `Username`, `First_Name`, `Last_Name`) VALUES ('U3', 'James321', 'James', 'Kirk');")

#mycursor.execute("INSERT INTO `movies`.`Ratings` (`Rating_ID`, `User_ID`, `Movie_ID`, `Rating`) VALUES ('U3M1', 'U3', 'M1', '7.9');")

#myresult = mycursor.fetchall()

# for x in myresult:
#     print(x)



# See PyCharm help at https://www.jetbrains.com/help/pycharm/

#change emails and the way username works use classes and stuff

#json return needed, look into apis, save into ratings table

# [{"title": "27 dresses", "rating": "5"},
# {"title": "titanic", "rating": "4,5"},
# {"title": "jab we met", "rating": "3.7"},
# {"title": "runaway bride", "rating": "4"},
# {"title": "pulp fiction", "rating": "4.3"}]
