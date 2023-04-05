from boto3 import resource
import config

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME
AWS_SESSION_TOKEN = config.AWS_SESSION_TOKEN



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
    