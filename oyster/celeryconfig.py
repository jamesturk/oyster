from oyster.conf import settings

CELERY_IMPORTS = ("oyster.tasks",)

BROKER_TRANSPORT = 'mongodb'
CELERY_RESULT_BACKEND = 'mongodb'

CELERY_MONGODB_BACKEND_SETTINGS = {
    'host': settings.MONGO_HOST,
    'port': settings.MONGO_PORT,
}
