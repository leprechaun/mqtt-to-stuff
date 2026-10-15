import logging

logger = logging.getLogger(__name__)


def generate_on_connect(topics):
    def on_connect(client, userdata, flags, reason_code, properties):
        for topic in topics:
            client.subscribe(topic)
        logger.info("Connected, subscribed to %s", topics)

    return on_connect
