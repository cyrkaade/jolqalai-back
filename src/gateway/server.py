import os, gridfs, pika, json
from flask import Flask, request, send_file
from flask_pymongo import PyMongo
from auth import validate
from auth_svc import access
from storage import util
from bson.objectid import ObjectId

server = Flask(__name__)

mongo_video = PyMongo(server, uri="mongodb://host.minikube.internal:27017/videos")

mongo_image = PyMongo(server, uri="mongodb://host.minikube.internal:27017/images")

fs_videos = gridfs.GridFS(mongo_video.db)
fs_images = gridfs.GridFS(mongo_image.db)

def get_rabbitmq_connection():
    try:
        # Setup connection parameters with increased heartbeat timeout.
        connection_params = pika.ConnectionParameters(
            "rabbitmq",
            heartbeat=600,  # Adjust the heartbeat timeout as necessary.
            blocked_connection_timeout=300  # Adjust connection timeout as necessary.
        )
        return pika.BlockingConnection(connection_params)
    except pika.exceptions.AMQPConnectionError as error:
        print(f"Failed to connect to RabbitMQ: {error}")
        # Implement retry logic here or raise to handle it at a higher level.
        raise

connection = get_rabbitmq_connection()
channel = connection.channel()


@server.route("/login", methods=["POST"])
def login():
    token, err = access.login(request)

    if not err:
        return token
    else:
        return err


@server.route("/upload", methods=["POST"])
def upload():
    access, err = validate.token(request)

    if err:
        return err

    access = json.loads(access)

    if access["admin"]:
        if len(request.files) > 1 or len(request.files) < 1:
            return "exactly 1 file required", 400

        for _, f in request.files.items():
            err = util.upload(f, fs_videos, channel, access)

            if err:
                return err

        return "success!", 200
    else:
        return "not authorized", 401


@server.route("/download", methods=["GET"])
def download():
    access, err = validate.token(request)
    if err:
        return err

    access = json.loads(access)
    if access["admin"]:
        fid_string = request.args.get("fid")
        if not fid_string:
            return "fid is required", 400

        try:
            out = fs_images.get(ObjectId(fid_string))  # Adjusted to fs_images
            return send_file(out, download_name=f"{fid_string}.jpg")  # Adjust the MIME type if necessary
        except Exception as err:
            print(err)
            return "internal server error", 500

    return "not authorized", 401



if __name__ == "__main__":
    server.run(host="0.0.0.0", port=8080)
