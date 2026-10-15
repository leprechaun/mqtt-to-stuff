import os


def build_storage_options(include_session_token=False):
    options = {}

    if s3_endpoint := os.environ.get('AWS_ENDPOINT_URL_S3'):
        options["endpoint_url"] = s3_endpoint

    if include_session_token:
        options["AWS_SESSION_TOKEN"] = os.environ.get('AWS_SESSION_TOKEN', "")

    return options
