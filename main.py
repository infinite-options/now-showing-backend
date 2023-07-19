# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

#from flask import Flask, request, render_template, url_for, redirect
#from flask_restful import Resource, Api
import json
import mysql.connector
from flask import Flask, request, jsonify
import pandas as pd
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
    print("Input data: ", movie)
    response = requests.get("https://api.themoviedb.org/3/search/movie?api_key=916893c3e71b0b37e324a142b99253f0&query={}".format(movie))
    response_data = response.json()
    if len(response_data['results'])<=1:
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
        mycursor.execute("INSERT INTO `Ratings`(`Rating_ID`, `User_ID`, `Movie_Name`, `User_Rating`, `Genres`, `Backdrop`, `Movie_ID`) VALUES(@record, %s, %s, %s, %s, %s, %s)",(user_id, response_data['original_title'], user_rating, genres, backdrop_path, id))
        cnx.commit()
        cnx.close()
        return 'ok'
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
    mycursor.execute("INSERT INTO `Ratings`(`Rating_ID`, `User_ID`, `Movie_Name`, `User_Rating`, `Genres`, `Backdrop`, `Movie_ID`) VALUES(@record, %s, %s, %s, %s, %s, %s)",(user_id, response_data['original_title'], user_rating, genres, backdrop_path, id))
    cnx.commit()
    cnx.close()
    return 'ok'
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