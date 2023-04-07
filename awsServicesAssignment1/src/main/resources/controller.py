from boto3 import resource
import config

AWS_ACCESS_KEY_ID = config.aws_access_key_id
AWS_SECRET_ACCESS_KEY = config.aws_secret_access_key
REGION_NAME = "us-east-1"
AWS_SESSION_TOKEN = config.aws_session_token



def create_table():
    dynamoDB = resource(
        'dynamodb',
        aws_access_key_id     = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
        aws_session_token = AWS_SESSION_TOKEN,
        region_name           = REGION_NAME
        )

    loginTable = dynamoDB.Table("login")    
    return loginTable

def read_login(loginTable, email):
    response = loginTable.get_item(
       Key = {
           'email': email
       },
       AttributesToGet = ['password', 'username']
   )
    return response

def write_email(loginTable, email, password, username):
    response = loginTable.put_item(
        Item = {
           'email'  : email,
           'password' : password,
           'username'  : username
           }
    )
    return response
    
def check_email(loginTable, email):
    response = loginTable.get_item(
       Key = {
           'email': email
       }, 
       
       AttributesToGet = ['email']
    )
    return response
    
def music_get(music_table, partition_key, sort_key):
    response = music_table.get_item(
       Key = {
           'Pkey': partition_key,
            'Skey': sort_key
       },
       AttributesToGet = ['title', 'artist', "year"]
   )
    return response