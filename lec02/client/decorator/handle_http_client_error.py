from functools import wraps
from loguru import logger
from requests.exceptions import Timeout, ConnectionError, RequestException, HTTPError


def handle_http_client_error(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValueError as e:
            logger.exception('Invalid response type.', e)
            raise
        except HTTPError as e:
            logger.exception('Bad request.', e)
            raise
        except Timeout as e:
            logger.exception('Timeout.', e)
            raise
        except ConnectionError as e:
            logger.exception('Connection error.', e)
            raise
        except RequestException as e:
            logger.exception('Connection error.', e)
            raise

    return wrapper
