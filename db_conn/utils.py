# Common Python library imports
import os
import time
from functools import wraps

# Pip package imports
from sshtunnel import SSHTunnelForwarder
from loguru import logger


def retry(ExceptionToCheck, tries=4, delay=1, backoff=2, logger=logger):
    """Retry calling the decorated function using an exponential backoff.

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            logger.error("%s, Retrying failed.")
            return None

        return f_retry  # true decorator

    return deco_retry


def get_nested(data, *args, **kwargs):
    if args and data:
        element  = args[0]
        if element:
            try:
                value = data.get(element)
            except AttributeError as err:
                logger.error("Exeption: %s Data: %s Args: %s" % (err, data, *args))
                return kwargs.get('default', None)
            else:
                return value if len(args) == 1 else get_nested(value, *args[1:])
    return kwargs.get('default', None)

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        #else:
#            cls._instances[cls].__init__(*args, **kwargs)
        return cls._instances[cls]
    """
    @staticmethod
    def getInstance():
        if Singleton.__instance == None:
            Singleton()
        return Singleton.__instance
    """


class Tunnel(SSHTunnelForwarder):

    config = {
        'ssh_address_or_host': (os.environ.get('SSH_ADDRESS'), os.environ.get('SSH_PORT')),
        'ssh_username': '',
        'ssh_password': '',
        'remote_bind_address':('127.0.0.1', 5432),
        'local_bind_address': ('127.0.0.1', 8080),  # could be any available port
    }

    def __init__(self, *args, **kwargs):

        config = { **Tunnel.config , **kwargs.get('config', {})}

        username = os.environ.get('SSH_USERNAME', kwargs.get('username', get_nested(config, 'ssh_username')))
        password = os.environ.get('SSH_PASSWORD', kwargs.get('password', get_nested(config, 'ssh_password')))

        super(Tunnel, self).__init__(
            get_nested(config, 'ssh_address_or_host'),
            ssh_username=username,
            ssh_password=password,
            remote_bind_address=get_nested(config, 'remote_bind_address'),
            local_bind_address=get_nested(config, 'local_bind_address',
            set_keepalive=10,
            mute_exceptions=True
                                          )
        )

    def __del__(self):
        self.close()

def listify(args):
    """Return args as a list.
    If already a list - returned as is.
    If a single instance of something that isn't a list, return it in a list.
    If "empty" (None or whatever), return a zero-length list ([]).
    """
    if args:
        if isinstance(args, list):
            return args
        return [args]
    return []