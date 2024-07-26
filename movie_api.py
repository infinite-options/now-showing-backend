# BOND BACKEND PYTHON FILE
# https://3s3sftsr90.execute-api.us-west-1.amazonaws.com/dev/api/v2/<enter_endpoint_details> for ctb


# SECTION 1:  IMPORT FILES AND FUNCTIONS
from flask import Flask, request, render_template, url_for, redirect, jsonify
from flask_restful import Resource, Api
from flask_mail import Mail, Message  # used for email
# used for serializer email and error handling
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadTimeSignature
from flask_cors import CORS

import boto3
import os.path
from urllib.parse import urlparse
from fuzzywuzzy import process
from decimal import Decimal
from datetime import datetime, date, timedelta
import os
import hashlib
import pytz
import pymysql
import requests
from datetime import datetime
from dotenv import load_dotenv
from io import BytesIO

import csv
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from io import StringIO

# from env_file import RDS_PW, S3_BUCKET, S3_KEY, S3_SECRET_ACCESS_KEY
s3 = boto3.client('s3')
# Load environment variables from the .env file
load_dotenv()

app = Flask(__name__)
cors = CORS(app, resources={r'/api/*': {'origins': '*'}})
# Set this to false when deploying to live application
app.config['DEBUG'] = True

# SECTION 2:  UTILITIES AND SUPPORT FUNCTIONS
# EMAIL INFO
# app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_SERVER'] = 'smtp.mydomain.com'
app.config['MAIL_PORT'] = 465

app.config['MAIL_USERNAME'] = 'support@manifestmy.space'
app.config['MAIL_PASSWORD'] = 'Support4MySpace'
app.config['MAIL_DEFAULT_SENDER'] = 'support@manifestmy.space'

app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True
# app.config['MAIL_DEBUG'] = True
# app.config['MAIL_SUPPRESS_SEND'] = False
# app.config['TESTING'] = False
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg'])
mail = Mail(app)
s = URLSafeTimedSerializer('thisisaverysecretkey')
# API
api = Api(app)

# convert to UTC time zone when testing in local time zone
utc = pytz.utc


# These statment return Day and Time in GMT
# def getToday(): return datetime.strftime(datetime.now(utc), "%Y-%m-%d")
# def getNow(): return datetime.strftime(datetime.now(utc),"%Y-%m-%d %H:%M:%S")

# These statment return Day and Time in Local Time - Not sure about PST vs PDT
def getToday(): return datetime.strftime(datetime.now(), "%Y-%m-%d")


def getNow(): return datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")


# NOTIFICATIONS - NEED TO INCLUDE NOTIFICATION HUB FILE IN SAME DIRECTORY
# from NotificationHub import AzureNotification
# from NotificationHub import AzureNotificationHub
# from NotificationHub import Notification
# from NotificationHub import NotificationHub
# For Push notification
# isDebug = False
# NOTIFICATION_HUB_KEY = os.environ.get('NOTIFICATION_HUB_KEY')
# NOTIFICATION_HUB_NAME = os.environ.get('NOTIFICATION_HUB_NAME')

# Twilio settings
# from twilio.rest import Client

# TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
# TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')


# SECTION 3: DATABASE FUNCTIONALITY
# RDS for AWS SQL 5.7
# RDS_HOST = 'pm-mysqldb.cxjnrciilyjq.us-west-1.rds.amazonaws.com'
# RDS for AWS SQL 8.0
RDS_HOST = 'io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com'
RDS_PORT = 3306
RDS_USER = 'admin'
RDS_DB = 'ctb'
RDS_PW = "prashant"  # Not sure if I need this
# RDS_PW = os.environ.get('RDS_PW')
S3_BUCKET = "manifest-image-db"


# S3_BUCKET = os.environ.get('S3_BUCKET')
# S3_KEY = os.environ.get('S3_KEY')
# S3_SECRET_ACCESS_KEY = os.environ.get('S3_SECRET_ACCESS_KEY')


# CONNECT AND DISCONNECT TO MYSQL DATABASE ON AWS RDS (API v2)
# Connect to MySQL database (API v2)
def connect():
    global RDS_PW
    global RDS_HOST
    global RDS_PORT
    global RDS_USER
    global RDS_DB

    # print("Trying to connect to RDS (API v2)...")
    try:
        conn = pymysql.connect(host=RDS_HOST,
                               user=RDS_USER,
                               port=RDS_PORT,
                               passwd=RDS_PW,
                               db=RDS_DB,
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)
        # print("Successfully connected to RDS. (API v2)")
        return conn
    except:
        print("Could not connect to RDS. (API v2)")
        raise Exception("RDS Connection failed. (API v2)")


# Disconnect from MySQL database (API v2)
def disconnect(conn):
    try:
        conn.close()
        # print("Successfully disconnected from MySQL database. (API v2)")
    except:
        print("Could not properly disconnect from MySQL database. (API v2)")
        raise Exception("Failure disconnecting from MySQL database. (API v2)")


# Execute an SQL command (API v2)
# Set cmd parameter to 'get' or 'post'
# Set conn parameter to connection object
# OPTIONAL: Set skipSerialization to True to skip default JSON response serialization
def execute(sql, cmd, conn, skipSerialization=False):
    response = {}
    # print("==> Execute Query: ", cmd,sql)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cmd == 'get':
                result = cur.fetchall()
                response['message'] = 'Successfully executed SQL query.'
                # Return status code of 280 for successful GET request
                response['code'] = 280
                if not skipSerialization:
                    result = serializeResponse(result)
                response['result'] = result
            elif cmd == 'post':
                conn.commit()
                response['message'] = 'Successfully committed SQL command.'
                # Return status code of 281 for successful POST request
                response['code'] = 281
            else:
                response['message'] = 'Request failed. Unknown or ambiguous instruction given for MySQL command.'
                # Return status code of 480 for unknown HTTP method
                response['code'] = 480
    except:
        response['message'] = 'Request failed, could not execute MySQL command.'
        # Return status code of 490 for unsuccessful HTTP request
        response['code'] = 490
    finally:
        # response['sql'] = sql
        return response


# Serialize JSON
def serializeResponse(response):
    try:
        for row in response:
            for key in row:
                if type(row[key]) is Decimal:
                    row[key] = float(row[key])
                elif (type(row[key]) is date or type(row[key]) is datetime) and row[key] is not None:
                    # Change this back when finished testing to get only date
                    row[key] = row[key].strftime("%Y-%m-%d")
                    # row[key] = row[key].strftime("%Y-%m-%d %H-%M-%S")
                # elif is_json(row[key]):
                #     row[key] = json.loads(row[key])
                elif isinstance(row[key], bytes):
                    row[key] = row[key].decode()
        return response
    except:
        raise Exception("Bad query JSON")


# RUN STORED PROCEDURES

# MOVE STORED PROCEDURES HERE


# Function to upload image to s3
def allowed_file(filename):
    # Checks if the file is allowed to upload
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def helper_upload_img(file):
    bucket = S3_BUCKET
    # creating key for image name
    salt = os.urandom(8)
    dk = hashlib.pbkdf2_hmac('sha256', (file.filename).encode(
        'utf-8'), salt, 100000, dklen=64)
    key = (salt + dk).hex()

    if file and allowed_file(file.filename):
        # image link
        filename = 'https://s3-us-west-1.amazonaws.com/' \
                   + str(bucket) + '/' + str(key)

        # uploading image to s3 bucket
        upload_file = s3.put_object(
            Bucket=bucket,
            Body=file,
            Key=key,
            ACL='public-read',
            ContentType='image/jpeg'
        )
        return filename
    return None


# Function to upload icons
def helper_icon_img(url):
    bucket = S3_BUCKET
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        raw_data = response.content
        url_parser = urlparse(url)
        file_name = os.path.basename(url_parser.path)
        key = 'image' + "/" + file_name

        try:

            with open(file_name, 'wb') as new_file:
                new_file.write(raw_data)

            # Open the server file as read mode and upload in AWS S3 Bucket.
            data = open(file_name, 'rb')
            upload_file = s3.put_object(
                Bucket=bucket,
                Body=data,
                Key=key,
                ACL='public-read',
                ContentType='image/jpeg')
            data.close()

            file_url = 'https://%s/%s/%s' % (
                's3-us-west-1.amazonaws.com', bucket, key)

        except Exception as e:
            print("Error in file upload %s." % (str(e)))

        finally:
            new_file.close()
            os.remove(file_name)
    else:
        print("Cannot parse url")

    return file_url


# RUN STORED PROCEDURES


#  -----------------------------------------  PROGRAM ENDPOINTS START HERE  -----------------------------------------

# MOVIE RECOMMENDATIONS

# genres = pd.read_csv('s3://now-showing/movies_genres.csv')
# ratings = pd.read_csv('s3://now-showing/movies_ratings.csv')

# ratings = pd.read_csv("/Users/anisha/Desktop/Infinite Options/Rec Sys/dataset/ratings.csv")
# genres = pd.read_csv("/Users/anisha/Desktop/Infinite Options/Rec Sys/dataset/movies.csv")

# ---------------------------------------------------------------------------------------------------------------
# s3_access_key = os.getenv('MW_KEY')
# s3_secret_key = os.getenv('MW_SECRET')
# s3_bucket_name = os.getenv('BUCKET_NAME')
# s3_file_key_ratings = os.getenv('S3_PATH_KEY_RATINGS')
# s3_file_key_genres = os.getenv('S3_PATH_KEY_GENRES')


# s3_client = boto3.client('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)
# # print("after s3 connect")

# genres_response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_key_genres)
# # print("the response of the S3", genres_response)
# # print()

# genres_csv_content = genres_response['Body'].read().decode('utf-8')
# genres = pd.read_csv(StringIO(genres_csv_content))

# ratings_response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_key_ratings)
# # print("the response of the S3", ratings_response)
# # print()

# # Read the Parquet file into a DataFrame
# parquet_body = BytesIO(ratings_response['Body'].read())
# ratings = pd.read_parquet(parquet_body)
# ---------------------------------------------------------------------------------------------------------------

# function to read from s3
def read_from_s3():
    # Get S3 credentials and bucket information from environment variables
    s3_access_key = os.getenv('MW_KEY')
    s3_secret_key = os.getenv('MW_SECRET')
    s3_bucket_name = os.getenv('BUCKET_NAME')
    s3_file_key_ratings = os.getenv('S3_PATH_KEY_RATINGS')
    s3_file_key_genres = os.getenv('S3_PATH_KEY_GENRES')

    # Initialize S3 client
    s3_client = boto3.client('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)

    # Read genres file
    genres_response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_key_genres)
    genres_csv_content = genres_response['Body'].read().decode('utf-8')
    genres = pd.read_csv(StringIO(genres_csv_content))

    # Read ratings file
    ratings_response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_key_ratings)
    parquet_body = BytesIO(ratings_response['Body'].read())
    ratings = pd.read_parquet(parquet_body)

    return genres, ratings


movie_id = 1  # default movie choice

# Global variables
rec_num = 10

# TMDB API key
API_KEY = os.getenv('TMDB_API_KEY')
API_KEY = "b8b76ccfa61c6e85ca7e096d905a7d63"


def fetch_tmdb_data(movie_title):
    print("In fetch_tmdb_data ", movie_title)
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
    genres, ratings = read_from_s3()

    # Find Other Users who liked the same movie
    similar_users = ratings[(ratings["movieId"] == movie_id) & (
            ratings["rating"] > 4)]["userId"].unique()
    # print("Similar Users: ", similar_users)

    # Find Other Movies that the Similar Users Liked
    similar_user_recs = ratings[(ratings["userId"].isin(
        similar_users)) & (ratings["rating"] > 4)]["movieId"]
    # print("Similar Users Recommendations: ", similar_user_recs)

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

    # print(rec_percentages.head(rec_num))
    # print(genres[genres['movieId'] == movie_id])
    # print(rec_percentages.head(rec_num).merge(genres, left_index=True, right_on="movieId"))

    return rec_percentages.head(rec_num).merge(genres, left_index=True, right_on="movieId")


class similar_recs(Resource):
    # print("In test endpoint")
    def get(self):
        print("in similar recommendations GET Movies")
        movies_response = {}

        try:
            conn = connect()

            query = """
                SELECT * 
                FROM bond.movies
                LEFT JOIN bond.girls
                    ON movie_uid = girl_movie_uid
                LEFT JOIN bond.songs
                    ON movie_uid = song_movie_uid;
            """

            movies = execute(query, 'get', conn)

            movies_response = movies['result']

            return (movies_response)
        except:
            print("Movies Endpoint Failed")
        finally:
            disconnect(conn)

    def post(self):
        # print("in similar recommendations POST Movies")
        # @app.route('/recommend', methods=['POST'])
        # def get_recommendations():
        data = request.json
        movie_id = data.get('movie_id')
        # print("Movie ID: ", movie_id)
        if not movie_id:
            return jsonify({"error": "No movie ID provided"}), 400

        try:
            recommendations = find_similar_movies(movie_id)
            # print("recommendations: ", recommendations)

            # Fetch TMDB data for each recommended movie
            for _, row in recommendations.iterrows():
                tmdb_data = fetch_tmdb_data(row['title'])
                if tmdb_data:
                    recommendations.loc[recommendations['title'] ==
                                        row['title'], 'tmdb_data'] = str(tmdb_data)

            # print("Before JSON")

            return jsonify(recommendations.to_dict(orient='records'))
        except Exception as e:
            return jsonify({"error": str(e)}), 500


# -- ---------------------------------------------------------------------


def clean_movie_datasets():
    # Preprocess the dataset
    # Remove movies that do not have any genres listed from both movies and ratings dataset
    # movies = movies[movies['genres'] != '(no genres listed)']
    genres, ratings = read_from_s3()
    genres_cleaned = genres[genres['genres'] != '(no genres listed)']

    # Remove users from the ratings dataset who have rated <= 200 movies
    above_threshold_users = ratings['userId'].value_counts() > 200
    above_threshold_user_indices = above_threshold_users[above_threshold_users].index
    ratings_cleaned = ratings[ratings['userId'].isin(above_threshold_user_indices)]

    # Count the number of ratings received by each movie
    num_rating = ratings_cleaned.groupby('movieId')['rating'].count().reset_index()
    num_rating.rename(columns={
        'rating': 'num_of_rating'
    }, inplace=True)

    # Remove the movies that have less than 50 ratings
    num_rating = num_rating[num_rating['num_of_rating'] >= 50]
    ratings_cleaned = ratings_cleaned.merge(num_rating, on='movieId')
    ratings_cleaned.drop_duplicates(['userId', 'movieId'], inplace=True)

    # Find the unique movieIds in the merged dataset
    unique_movie_ids = ratings_cleaned['movieId'].unique()

    # Keep only unique_movie_ids in the movies and ratings dataset
    genres_cleaned = genres_cleaned[genres_cleaned['movieId'].isin(unique_movie_ids)]
    ratings_cleaned = ratings_cleaned[ratings_cleaned['movieId'].isin(unique_movie_ids)]

    # Sort the datasets by movieId
    genres_cleaned.sort_values('movieId')
    ratings_cleaned.sort_values('movieId')

    return genres_cleaned, ratings_cleaned


class profile_recs(Resource):
    def post(self):
        # print("In Profile Recommendation endpoint")
        # @app.route('/recommend', methods=['POST'])
        # def recommend():
        genres_cleaned, ratings_cleaned = clean_movie_datasets()
        data = dict(request.json)
        # Read and cast the movie ratings dictionary
        user_ratings = {int(k): float(v) for k, v in data['ratings'].items()}

        # Collaborative filtering using rating
        user_item_matrix = ratings_cleaned.pivot_table(columns='userId', index='movieId', values='rating').fillna(0)

        # print("User ratings: ")
        # print(user_ratings)
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
        recommended_movie_details = genres_cleaned[genres_cleaned['movieId'].isin(recommended_movie_ids)]

        # Calculate the average rating for each movie
        average_ratings = ratings_cleaned.groupby('movieId')['rating'].mean()

        recommended_movie_details = recommended_movie_details.merge(average_ratings, on='movieId')
        recommended_movie_details.rename(columns={
            'rating': 'avg_rating'
        }, inplace=True)
        recommendations = recommended_movie_details[['movieId', 'title', 'genres', 'avg_rating']]

        try:
            return jsonify(recommendations.to_dict(orient='records'))
        except Exception as e:
            return jsonify({"error": str(e)}), 500


class find_movie_title(Resource):
    def post(self):
        data = request.get_json()
        genres_cleaned, ratings_cleaned = clean_movie_datasets()
        movie_title = data.get('title')
        # Check for exact match
        exact_match = genres_cleaned[genres_cleaned['title'].str.lower() == movie_title.lower()]
        if not exact_match.empty:
            result = {
                'title': exact_match['title'].values[0],
                'movieId': int(exact_match['movieId'].values[0])
            }
            return jsonify({'exact_match': result})

        # Find the top 5 matching movie titles
        matches = process.extract(movie_title, genres_cleaned['title'], limit=5)
        top_5_titles = [
            {'title': match[0],
             'movieId': int(genres_cleaned[genres_cleaned['title'] == match[0]]['movieId'].values[0])}
            for match in matches
        ]

        return jsonify({'matches': top_5_titles})


#  -- ACTUAL ENDPOINTS    -----------------------------------------

# New APIs, uses connect() and disconnect()
# Create new api template URL
# api.add_resource(TemplateApi, '/api/v2/templateapi')

# Run on below IP address and port
# Make sure port number is unused (i.e. don't use numbers 0-1023)

# GET requests
api.add_resource(similar_recs, '/api/v2/similar')
api.add_resource(profile_recs, '/api/v2/profile')
api.add_resource(find_movie_title, '/api/v2/findMovieTitle')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)