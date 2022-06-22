import os
from google.cloud import pubsub_v1
import google
from config import *

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"credentials.json"


def create_topic():
    try:
        topic_name = 'projects/{project_id}/topics/{topic}'.format(
            project_id=GOOGLE_CLOUD_PROJECT,
            topic=TOPIC_NAME,  # Set this to something appropriate.
        )
        publisher.create_topic(name=topic_name)
    except google.api_core.exceptions.AlreadyExists as err:
        print("Something went wrong: {}".format(err))
    except Exception as exc:
        print(exc)
    finally:
        return topic_name


def publish_message_to_topic(message):
    try:
        future = publisher.publish(topic_name, message.encode("utf-8"), spam='eggs')
        future.result()
    except Exception as exc:
        print(exc)


def callback(message):
    print(message.data)
    message.ack()


def create_subscriber():
    try:
        subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
            project_id=GOOGLE_CLOUD_PROJECT,
            sub=SUBSCRIPTION_NAME,  # Set this to something appropriate.
        )
        with pubsub_v1.SubscriberClient() as subscriber:
            subscriber.create_subscription(
                name=subscription_name, topic=topic_name)
            future = subscriber.subscribe(subscription_name, callback)
    except Exception as exc:
        print(exc)


publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

topic_name = create_topic()
publish_message_to_topic('First message')
create_subscriber()


