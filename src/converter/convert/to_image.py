import pika, json, tempfile, os
from bson.objectid import ObjectId
import moviepy.editor
import numpy as np
# from your_model import detect_pits  # Assuming this is your image processing model
import cv2

def start(message, fs_videos, fs_images, channel):
    message = json.loads(message)

    # Temporary file for video
    tf = tempfile.NamedTemporaryFile(delete=False)
    out = fs_videos.get(ObjectId(message["video_fid"]))
    tf.write(out.read())
    tf.seek(0)

    # Process video to extract an image
    video = moviepy.editor.VideoFileClip(tf.name)
    # Assuming you want to capture a frame from the first second of the video
    frame = video.get_frame(1)  # Get a frame at t=1 second

    # Convert frame to image and save
    image_path = tempfile.gettempdir() + f"/{message['video_fid']}.jpg"
    cv2.imwrite(image_path, cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))

    # Save image to MongoDB
    with open(image_path, "rb") as f:
        data = f.read()
        fid = fs_images.put(data)  # Assuming fs_images is your GridFS bucket for images

    message["image_fid"] = str(fid)
    os.remove(tf.name)  # Clean up temporary video file
    os.remove(image_path)  # Clean up saved image

    # Publish message with image_fid
    try:
        channel.basic_publish(
            exchange="",
            routing_key=os.environ.get("IMAGE_QUEUE"),  # Make sure to define IMAGE_QUEUE in your config
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
        )
    except Exception as err:
        fs_images.delete(fid)  # Cleanup if publishing fails
        print(f"Failed to publish message: {err}")
        return "failed to publish message"