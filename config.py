# config.py
import os


class Config:
    MYSQL_HOST = os.getenv('MYSQL_HOST', "io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com")
    MYSQL_USER = os.getenv('MYSQL_USER', "admin")
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', "prashant")
    MYSQL_DB = os.getenv('MYSQL_DB', "movies")
    MYSQL_PORT = os.getenv('MYSQL_PORT', 3306)
