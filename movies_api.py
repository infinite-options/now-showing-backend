# POKER BACKEND PYTHON FILE
# https://3s3sftsr90.execute-api.us-west-1.amazonaws.com/dev/api/v2/<enter_endpoint_details> for ctb


# SECTION 1:  IMPORT FILES AND FUNCTIONS
from flask import Flask, request, render_template, url_for, redirect
from flask_restful import Resource, Api
from flask_mail import Mail, Message  # used for email
# used for serializer email and error handling
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadTimeSignature
from flask_cors import CORS

import boto3
import os.path

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from urllib.parse import urlparse
import urllib.request
import base64
from oauth2client import GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI, client
from io import BytesIO
from pytz import timezone as ptz
import pytz
from dateutil.relativedelta import relativedelta
import math

from werkzeug.exceptions import BadRequest, NotFound

from dateutil.relativedelta import *
from decimal import Decimal
from datetime import datetime, date, timedelta
from hashlib import sha512
from math import ceil
import string
import random
import os
import hashlib

# regex
import re
# from env_keys import BING_API_KEY, RDS_PW

import decimal
import sys
import json
import pytz
import pymysql
import requests
import stripe
import binascii
from datetime import datetime
import datetime as dt
from datetime import timezone as dtz
import time

import csv


# from env_file import RDS_PW, S3_BUCKET, S3_KEY, S3_SECRET_ACCESS_KEY
s3 = boto3.client('s3')


app = Flask(__name__)
cors = CORS(app, resources={r'/api/*': {'origins': '*'}})
# Set this to false when deploying to live application
app.config['DEBUG'] = True





# SECTION 2:  UTILITIES AND SUPPORT FUNCTIONS
# EMAIL INFO
#app.config['MAIL_SERVER'] = 'smtp.gmail.com'
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
RDS_PW="prashant"   # Not sure if I need this
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
    dk = hashlib.pbkdf2_hmac('sha256',  (file.filename).encode(
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





# -- Stored Procedures start here -------------------------------------------------------------------------------


# RUN STORED PROCEDURES

def get_new_matrixUID(conn):
    newMatrixQuery = execute("call poker.new_matrix_uid()", 'get', conn)
    if newMatrixQuery['code'] == 280:
        return newMatrixQuery['result'][0]['new_id']
    return "Could not generate new game UID", 500


#  -----------------------------------------  PROGRAM ENDPOINTS START HERE  -----------------------------------------


# SELECT MOVIES
class pre_flop_tables(Resource):
    def get(self):
        print("in Pre_Flop")
        movies_response = {}

        try:
            conn = connect()

            query = """
                SELECT * FROM poker.pre_flop_decision_table
            """

            pre_flop = execute(query, 'get', conn)

            pre_flop_result = pre_flop['result']

            return(pre_flop_result)
        except:
            print("Pre Flop Endpoint Failed")
        finally:
            disconnect(conn)

    def post(self):
        response = {}
        items = {}
        try:
            conn = connect()
            data = request.get_json(force=True)
            # print to Received data to Terminal
            print("Received:", data)

            # preflop_uid = data["preflop_uid"]
            player_type = data["player_type"]
            position  = data["position"]
            preflop_table = json.dumps(data["preflop_table"])
            


            new_preflop_uid = get_new_matrixUID(conn)
            print(new_preflop_uid)
            print(getNow())


            query = """
                INSERT INTO poker.pre_flop_decision_table 
                SET preflop_uid = \'""" + new_preflop_uid + """\',
                    player_type = \'""" + player_type + """\',
                    position = \'""" + position + """\',
                    preflop_table = \'""" + preflop_table + """\'
                    """

            # print(query)
            items = execute(query, "post", conn)
            print("items: ", items)
            if items["code"] == 281:
                response["message"] = "Create Table successful"
                response["new UID"] = new_preflop_uid
                # response["game_code"] = str(game_code)
                # response["game_uid"] = str(new_game_uid)
                return response, 200
        except:
            raise BadRequest("Create Table Request failed")
        finally:
            disconnect(conn)

class pre_flop_tables_post(Resource):
    def post(self):
        response = {}
        items = {}
        try:
            conn = connect()
            data = request.get_json(force=True)
            # print to Received data to Terminal
            print("Received:", data)

            # preflop_uid = data["preflop_uid"]
            player_type = data["player_type"]
            position  = data["position"]
            preflop_table = data["preflop_table"]
            


            new_preflop_uid = get_new_matrixUID(conn)
            print(new_preflop_uid)
            print(getNow())

            # user_uid = data["user_uid"]
            # num_rounds = data["rounds"]
            # time_limit = data["round_time"]
            # scoring = data["scoring_scheme"]
            # print(user_uid)

            # new_game_uid = get_new_gameUID(conn)
            # print(new_game_uid)
            # print(getNow())

            # game_code = random.randint(10000000, 99999999)
            # print(game_code)

            query = """
                INSERT INTO poker.pre_flop_decision_table 
                SET preflop_uid = \'""" + new_preflop_uid + """\',
                    player_type = \'""" + player_type + """\',
                    position = \'""" + position + """\',
                    preflop_table = \"""" + preflop_table + """\"
                    """

            # query = '''
            #     INSERT INTO captions.game
            #     SET game_uid = \'''' + new_game_uid + '''\',
            #         game_created_at = \'''' + getNow() + '''\',
            #         game_code = \'''' + str(game_code) + '''\',
            #         num_rounds = \'''' + num_rounds + '''\',
            #         time_limit = \'''' + time_limit + '''\',
            #         game_host_uid = \'''' + user_uid + '''\',
            #         scoring_scheme = \'''' + scoring + '''\'
            #     '''

            print(query)
            items = execute(query, "post", conn)
            print("items: ", items)
            if items["code"] == 281:
                response["message"] = "Create Table successful"
                response["new UID"] = new_preflop_uid
                # response["game_code"] = str(game_code)
                # response["game_uid"] = str(new_game_uid)
                return response, 200
        except:
            raise BadRequest("Create Table Request failed")
        finally:
            disconnect(conn)





#  -- GRATIS RELATED FUNCTIONS     -----------------------------------------

# SELECT MOVIES
class movies(Resource):
    def get(self):
        print("in Movies")
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

            return(movies_response)
        except:
            print("Movies Endpoint Failed")
        finally:
            disconnect(conn)

# SELECT MOVIES TITLES
class movietitles(Resource):
    def get(self):
        print("in Movies")
        movies_response = {}

        try:
            conn = connect()

            query = """
                SELECT movie_order, movie_title
                FROM bond.movies;
            """

            movies = execute(query, 'get', conn)

            movies_response = movies['result']

            return(movies_response)
        except:
            print("Movies Endpoint Failed")
        finally:
            disconnect(conn)


# SELECT BOND GIRLS
class girls(Resource):
    def get(self):
        print("in Movies")
        girls_response = {}

        try:
            conn = connect()

            query = """
                SELECT * 
                FROM bond.girls
                LEFT JOIN bond.movies
                    ON movie_uid = girl_movie_uid;
            """

            girls = execute(query, 'get', conn)

            girls_response = girls['result']

            return(girls_response)
        except:
            print("Movies Endpoint Failed")
        finally:
            disconnect(conn)


# SELECT BOND VILLAINS
class villains(Resource):
    def get(self):
        print("in Movies")
        villains_response = {}

        try:
            conn = connect()

            query = """
                SELECT * 
                FROM bond.villains
                LEFT JOIN bond.movies
                    ON movie_uid = villain_movie_uid;
            """

            villains = execute(query, 'get', conn)

            villains_response = villains['result']

            return(villains_response)
        except:
            print("Movies Endpoint Failed")
        finally:
            disconnect(conn)


# SELECT BOND SIDEKICKS
class sidekicks(Resource):
    def get(self):
        print("in Movies")
        sidekicks_response = {}

        try:
            conn = connect()

            query = """
                SELECT * 
                FROM bond.movies
                LEFT JOIN bond.villains
                    ON movie_uid = villain_movie_uid
                LEFT JOIN bond.sidekicks
                    ON movie_uid = sidekick_movie_uid
                WHERE sidekick IS NOT NULL;
            """

            # Query Response
            sidekicks = execute(query, 'get', conn)
            # print(sidekicks)
            sidekicks_response = sidekicks['result']
            # print(sidekicks_response)

            return(sidekicks_response)
        except:
            print("Movies Endpoint Failed")
        finally:
            disconnect(conn)


# SELECT BOND SONGS
class songs(Resource):
    def get(self):
        print("in Movies")
        songs_response = {}

        try:
            conn = connect()

            query = """
                SELECT * 
                FROM bond.songs
                LEFT JOIN bond.movies
                    ON movie_uid = song_movie_uid;
            """

            songs = execute(query, 'get', conn)

            songs_response = songs['result']

            return(songs_response)
        except:
            print("Movies Endpoint Failed")
        finally:
            disconnect(conn)







#  -- ACTUAL ENDPOINTS    -----------------------------------------

# New APIs, uses connect() and disconnect()
# Create new api template URL
# api.add_resource(TemplateApi, '/api/v2/templateapi')

# Run on below IP address and port
# Make sure port number is unused (i.e. don't use numbers 0-1023)

# GET requests
api.add_resource(pre_flop_tables, '/api/v2/preflop')
api.add_resource(pre_flop_tables_post, '/api/v2/preflop_post')

api.add_resource(movies, '/api/v2/movies')
api.add_resource(girls, '/api/v2/girls')
api.add_resource(villains, '/api/v2/villains')
api.add_resource(sidekicks, '/api/v2/sidekicks')
api.add_resource(songs, '/api/v2/songs')
api.add_resource(movietitles, '/api/v2/movietitles')



if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)
