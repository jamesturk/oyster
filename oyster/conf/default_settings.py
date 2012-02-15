# mongodb
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DATABASE = 'oyster'
MONGO_LOG_MAXSIZE = 100000000

# extra celery modules
CELERY_TASK_MODULES = []

# scrapelib
USER_AGENT = 'oyster'
REQUESTS_PER_MINUTE = 60
REQUEST_TIMEOUT = 300

# other
RETRY_ATTEMPTS = 3
RETRY_WAIT_MINUTES = 60

DOCUMENT_CLASSES = {}
