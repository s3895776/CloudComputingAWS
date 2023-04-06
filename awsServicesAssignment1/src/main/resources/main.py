# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START gae_python38_render_template]
# [START gae_python3_render_template]
import datetime

from flask import Flask, render_template, request, redirect
import controller as dynamodb

app = Flask(__name__)
    
@app.route('/')
def root():
    
    # receive post request 
    return render_template('login.html', submit = False)

@app.route('/register', methods = ["GET", "POST"])
def register_page():
    return render_template("register.html", submit = False)

@app.route('/register/attempt', methods = ["POST"])
def registering():
    loginTable = dynamodb.create_table()
    
    email = request.form['email']

    submit = True
    missing_information = False
    registered = False

    if (email == ""):
        missing_information = True
        # TODO: set some html for no missing information. 
        return render_template("register.html",
                                submit= submit, registered= registered, 
                                missing_information = missing_information )
    checkEmail = dynamodb.check_email(loginTable, email)
    
    if (checkEmail['ResponseMetadata']['HTTPStatusCode'] == 200):

        if ('Item' in checkEmail):

            registered = True

            return render_template("register.html",
                                submit= submit, registered= registered, 
                                missing_information = missing_information)

        else:
            password = request.form['password']
            username = request.form['username']

            # TODO: set html for missing information. 
            if username == "": 
                missing_information = True
                return render_template("register.html",
                                submit= submit, registered= registered, 
                                missing_information = missing_information)
            elif password == "":
                missing_information = True
                return render_template("register.html",
                                submit= submit, registered= registered, 
                                missing_information = missing_information)
            else:
                response = dynamodb.write_email(loginTable, email, password, username)
                return redirect("/")
            

    else:
        return {
            "msg": "error",
            "response": response['ResponseMetadata']['HTTPStatusCode']
        }  


@app.route('/', methods=['POST'])
def login_post():
    loginTable = dynamodb.create_table()
    email = request.form['email']
    password = request.form['password']
    
    if (email == ""):
        return render_template('login.html', invalid = True, submit = True)
    
    # Add your authentication logic here
    response = dynamodb.read_login(loginTable, email)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):

        if ('Item' in response):

            if ('password' in response['Item']):
            # validate password
                if (response['Item']['password'] == password ):
                    
                    # this posts a get request. 
                    return redirect('main' + "/" + response['Item']['username'] + "" )
                
                
                else:
                    return render_template('login.html', invalid = True, submit = True)
            else:
                return "should not reach here"

        else:
            return render_template('login.html', invalid = True, submit = True)
        
    return {
        "msg": "error",
        "response": response['ResponseMetadata']['HTTPStatusCode']
    }  

@app.route('/main/<username>')
def main_page(username):
    return render_template("main.html", username = username)

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python3_render_template]
# [END gae_python38_render_template]
