from oyster.conf import settings

CELERY_IMPORTS = ("oyster.tasks",)

BROKER_TRANSPORT = 'mongodb'
BROKER_HOST = settings.MONGO_HOST
BROKER_PORT = settings.MONGO_PORT

CELERY_RESULT_BACKEND = 'mongodb'
CELERY_MONGODB_BACKEND_SETTINGS = {
    'host': settings.MONGO_HOST,
    'port': settings.MONGO_PORT,
}
