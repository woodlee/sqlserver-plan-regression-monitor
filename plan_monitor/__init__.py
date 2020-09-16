import logging.config
import os

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        __name__: {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': True,
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': LOG_LEVEL,
            'formatter': 'simple',
        }
    },
    'formatters': {
        'simple': {
            'format': '%(asctime)s %(levelname)-8s (%(processName)s) [%(name)s:%(lineno)s] %(message)s',
        }
    }
})
