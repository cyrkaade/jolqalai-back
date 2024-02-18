import pika, sys, os
from pymongo import MongoClient
import gridfs
from converter.convert import to_image  # Ensure this is correctly imported

def main():
    client = MongoClient("host.minikube.internal", 27017)
    db_videos = client['videos']
    db_images = client['images']  # Assuming you have a separate database or collection for images
    fs_videos = gridfs.GridFS(db_videos)
    fs_images = gridfs.GridFS(db_images)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()

    def callback(ch, method, properties, body):
        err = to_image.start(body, fs_videos, fs_images, ch)
        if err:
            ch.basic_nack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue=os.environ.get("VIDEO_QUEUE"), on_message_callback=callback
    )

    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
