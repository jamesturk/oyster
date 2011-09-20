CELERY_IMPORTS = ("oyster.tasks",)

BROKER_TRANSPORT = 'mongodb'
CELERY_RESULT_BACKEND = 'mongodb'
CELERY_MONGODB_BACKEND_SETTINGS = {
    'host': 'localhost',
    'port': 27017,
}
