import os
import json
import requests

import boto.sqs
import boto.sqs.queue
from boto.sqs.message import Message
from boto.sqs.connection import SQSConnection
from boto.exception import SQSError

from flask import Flask, request, redirect, url_for
from werkzeug import secure_filename

UPLOAD_FOLDER = '/data/'
ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def connect():

    res = requests.get('http://ec2-52-30-7-5.eu-west-1.compute.amazonaws.com:81/key')
    keys = res.text.split(':')

    # Get the keys from a specific url and then use them to connect to AWS Service 
    access_key_id = keys[0]
    secret_access_key = keys[1]

    # Set up a connection to the AWS service. 
    return boto.sqs.connect_to_region("eu-west-1", aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)


@app.route("/")
def index():
   return "ay"


@app.route('/queues', methods=['GET'])
def list_all_queues():
    conn = connect()

    rs = conn.get_all_queues()

    queues = []

    for q in rs:
        queue = {}
        queue['name'] = q.id
        queues.append(queue)

    return json.dumps(queues)

@app.route('/queues', methods=['POST'])
def create_queue():

    body = request.get_json(force=True)
    name = body['name'] 

    conn = connect()
    queue = conn.create_queue(name)

    resp = {}
    resp['name'] = queue.name

    return json.dumps(resp)


@app.route('/queues/<name>', methods=['DELETE'])
def delete_queue(name):

    conn = connect()

    try:
        queue = conn.get_queue(name)

        if conn.delete_queue(queue):
            return "Queue deleted: " + name
        else:
            return "Failed to delete: " + name

    except:
        return "Queue does not exist: " + name


@app.route('/queues/<name>/msgs/count', methods=['GET'])
def count_messages(name):
    conn = connect()

    # Get the queue object to count the messages
    queue = conn.get_queue(name)

    # Count the messages
    if queue is not None:
        attr = queue.get_attributes()
        return "Number of messages in " + queue.name + ": " + attr['ApproximateNumberOfMessages']
    else:
        return "Queue could not be found"


@app.route('/queues/<name>/msgs', methods=['POST'])
def write_message(name):

    body = request.get_json(force=True)
    message = body['content'] 

    conn = connect()
    queue = conn.get_queue(name)
    
    # Send the message
    if queue is not None:
        conn.send_message(queue, message)
        return "Sent message to: " + queue.name
    else:
        return "Unable to send message"



@app.route('/queues/<name>/msgs', methods=['GET'])
def read_message(name):

    conn = connect()
    queue = conn.get_queue(name)
    
    # Read the first message
    if queue is not None:
        message = queue.read()
        return "Messgae: " + message.get_body()
    else:
        return "Queue could not be found"


@app.route('/queues/<name>/msgs', methods=['DELETE'])
def consume_message(name):

    conn = connect()
    queue = conn.get_queue(name)

    # Consume the message
    if queue is not None:
        message = queue.read()
        return "Message consumed: " + message.get_body()
    else:
        return "Queue could not be found"



if __name__ == "__main__":
    app.run(host="0.0.0.0")
