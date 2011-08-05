from oyster.conf import default_settings

class Settings(object):
    def __init__(self):
        pass

    def update(self, module):
        for setting in dir(module):
            if setting.isupper():
                val = getattr(module, setting)
                if val is not None:
                    setattr(self, setting, val)

settings = Settings()
settings.update(default_settings)

try:
    import oyster_settings
    settings.update(oyster_settings)
except ImportError:
    pass
