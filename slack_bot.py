import pandas as pd
import boto3
import os
import tempfile
import json
from slack import WebClient
from flask import Flask, request, make_response
from datetime import datetime

# A function to call data from S3 bucket
# It will call the latest corona related number based on todays date.
def call_data():

    # create a boto3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id = os.environ['BEE_AWS_ID'],
        aws_secret_access_key = os.environ['BEE_AWS_PW']
    )
    
    # create a temporary directory where files will be temporarily downloaded
    tempdir = tempfile.TemporaryDirectory()

    # todays date in string
    today = str(datetime.today().date()).replace('-','')

    # download the corona related counts from s3 and upload it into the temporary directory
    s3.download_file(
        Bucket = 'beetestawsbucket',
        Key = f'corona/yyyymmdd={today}/{today}.parquet',
        Filename = f'{tempdir.name}/tempfile.parquet'
    )

    # Read the parquet data file from the directory and turn it into a dataframe using pandas
    temp_df = pd.read_parquet(f'{tempdir.name}/tempfile.parquet')

    # Among multiple stacks of data choose the latest corona related count
    recent_df = temp_df.head(1)

    # Save data into a dictionary. Column name = key and Count = value
    variables={}
    for columns in recent_df:
        tempvar = {columns: recent_df[columns].values[0]}
        variables.update(tempvar)
    
    return variables

token = os.environ['SLACK_BOT_TOKEN']
client = WebClient(token)

app = Flask(__name__)

# The following is the function that handles the event
def event_handler(event_type, slack_event):
    # if the slackbot has been mentioned
    if event_type == "app_mention":
        # Find out what channel the slack has been called through
        channel = slack_event["event"]["channel"]
        # Have the crawled data in the variable text
        text = call_data()
        messageback = f"""The following is the number of corona cases for today:
data has been last updated at:      {text.get('update_time')}
Total world cases :                 {text.get('world_cases')}
New world cases for today :         {text.get('world_new_cases')}
Total world mortality :             {text.get('world_mortality')}
New world mortality for today :     {text.get('world_new_mortality')}
Total cases in Korea :              {text.get('korea_cases')}
New cases in Korea for today :      {text.get('korea_new_cases')}
Total mortality in Korea :          {text.get('korea_mortality')}
New mortality in Korea for today :  {text.get('korea_new_mortality')}
                           """
        # Post back the data to the channel where it has been mentioned
        client.chat_postMessage(channel = channel, text = messageback)
        return make_response("app mention message is sent",200)
    
    # If response is not made, throw an error 
    message = "[%s] cant find event handler" % event_type
    return make_response(message,200)

# Create a URL path which handles APIs from SLACK.
# Response when API come in as GET or POST method
@app.route("/slack", methods = ["GET","POST"])
def hears(): 
    # Receive json data from slack and put it in varaible slack_event
    slack_event = json.loads(request.data)
    print(slack_event)

    # If challenge message is slack_event exists, send back the same message 
    # This lets the slack sever know the connection is successful
    if "challenge" in slack_event:
        return make_response(slack_event["challenge"], 200)

    # Check for the event exist in the message
    if "event" in slack_event:
        # put the event and event type in variable event_type
        event_type = slack_event["event"]["type"]
        # Make the event handler function take care of the event
        return event_handler(event_type, slack_event)
        # If there is no event in the slack_events, throw an error message
    return make_response("no event made", 404)

# The following is a URL path just to check if the webserver works
@app.route("/", methods = ["GET","POST"])
def index():
    return "Hello World"

if __name__ == '__main__':
    print(call_data())
    app.run('0.0.0.0', port = 8080)