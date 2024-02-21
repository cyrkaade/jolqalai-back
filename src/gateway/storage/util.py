import pika, json, logging
from pika.exceptions import AMQPConnectionError, AMQPChannelError



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


# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def upload(f, fs, channel, access):
    fid = None
    try:
        # Attempt to store the file and get its ID
        fid = fs.put(f)
        logging.info(f"File stored successfully with id: {fid}")
    except Exception as err:
        logging.error(f"Error storing file: {err}")
        return "internal server error", 500

    message = {
        "video_fid": str(fid),
        "image_fid": None,
        "username": access["username"],
    }

    try:
        # Attempt to publish the message to the queue
        channel.basic_publish(
            exchange="",
            routing_key="video",
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ),
        )
        logging.info("Message published successfully")
    except (AMQPConnectionError, AMQPChannelError) as e1:
        logging.error(f"RabbitMQ publish error: {e1}, attempting to republish...")
        # Try to reconnect and create a new channel
        connection = get_rabbitmq_connection()  # Re-establish connection
        channel = connection.channel()  # Create a new channel
        try:
            channel.basic_publish(
                exchange="",
                routing_key="video",
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ),
            )
            logging.info("Message republished successfully")
        except Exception as e2:
            logging.error(f"Failed to republish message: {e2}")
            if fid:
                fs.delete(fid)
            return "internal server error on republish", 500
    except Exception as err:
        logging.error(f"General error when publishing message: {err}")
        if fid:
            fs.delete(fid)
        return "internal server error", 500
