import argparse
import sys

def main(args):
    parser = argparse.ArgumentParser(description="Copy MQTT events to stdout.")
    parser.add_argument("mqtt_topic", help="The MQTT topic to subscribe to.")
    # ai! the second argument should be mqtt_host
    parser.add_argument("output_format", help="The output format (e.g., json, text).")

    args = parser.parse_args()

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
