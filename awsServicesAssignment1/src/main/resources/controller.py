from boto3 import resource
import config

AWS_ACCESS_KEY_ID = config.aws_access_key_id
AWS_SECRET_ACCESS_KEY = config.aws_secret_access_key
REGION_NAME = "us-east-1"
AWS_SESSION_TOKEN = config.aws_session_token



def create_login_table():
    dynamoDB = resource(
        'dynamodb',
        aws_access_key_id     = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
        aws_session_token = AWS_SESSION_TOKEN,
        region_name           = REGION_NAME
        )

    loginTable = dynamoDB.Table("login")    
    return loginTable

def create_music_table():
    dynamoDB = resource(
        'dynamodb',
        aws_access_key_id     = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
        aws_session_token = AWS_SESSION_TOKEN,
        region_name           = REGION_NAME
        )

    musicTable = dynamoDB.Table("music")    
    return musicTable



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
    
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Scan.html
# sourced from amazon sdk 
def scan_music(music_table, title, artist, year):

    ExpressionAttributeNames = dict()
    ExpressionAttributeNames["#yr"] = "year"

    ExpressionAttributeValues = dict()

    FilterExpressionArray = list()
    BOOL_TITLE_EMPTY = title == ""
    BOOL_ARTIST_EMPTY = artist == ""
    BOOL_YEAR_EMPTY = year == ""

    if (not BOOL_TITLE_EMPTY):
        ExpressionAttributeValues[':t'] = {'S':title}
        FilterExpressionArray.append("title = :t")
    
    if (not BOOL_ARTIST_EMPTY):
        ExpressionAttributeValues[':a'] = {'S':artist}
        FilterExpressionArray.append("artist = :a")

    if (not BOOL_YEAR_EMPTY):
        ExpressionAttributeValues[':yr'] = {'N':year}
        FilterExpressionArray.append("#yr = :yr")
    

    FilterExpression = ""

    for word in FilterExpressionArray:
        if ( not (FilterExpression == "") ):
            FilterExpression += " and "
        FilterExpression += word

    ProjectionExpression = ""
    ProjectionExpressionArray = list()
    
    ProjectionExpressionArray.append("#yr")

    for word in ProjectionExpressionArray:
        if ( not (ProjectionExpression == "") ):
            ProjectionExpression += ", "
        ProjectionExpression += word
    
    print("ExpressionAttributeNames=",ExpressionAttributeNames)
    print("ExpressionAttributeValues=",ExpressionAttributeValues)
    print("FilterExpression=",FilterExpression)
    print("ProjectionExpression=",ProjectionExpression)

    if (BOOL_TITLE_EMPTY and BOOL_ARTIST_EMPTY and BOOL_YEAR_EMPTY):
        return "empty"

    response = music_table.scan(
        ExpressionAttributeNames = ExpressionAttributeNames,
        ExpressionAttributeValues = ExpressionAttributeValues,
        FilterExpression = FilterExpression,
        ProjectionExpression = ProjectionExpression,
        TableName = "music"
    )
    return response
