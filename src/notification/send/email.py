import smtplib, os, json
from email.message import EmailMessage


def notification(message):
    # try:
    message = json.loads(message)
    image_fid = message["image_fid"]
    sender_address = os.environ.get("GMAIL_ADDRESS")
    sender_password = os.environ.get("GMAIL_PASSWORD")
    receiver_address = message["username"]

    msg = EmailMessage()
    msg.set_content(f"image file_id: {image_fid} is now ready!")
    msg["Subject"] = "Image Download"
    msg["From"] = sender_address
    msg["To"] = receiver_address

    session = smtplib.SMTP("smtp.gmail.com", 587)
    session.starttls()
    session.login(sender_address, sender_password)
    session.send_message(msg, sender_address, receiver_address)
    session.quit()
    print("Mail Sent")


# except Exception as err:
# print(err)
# return err
