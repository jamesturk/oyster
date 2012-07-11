from oyster.conf import settings

CELERY_IMPORTS = ['oyster.tasks'] + list(settings.CELERY_TASK_MODULES)
