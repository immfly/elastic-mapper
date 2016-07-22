from elastic_mapper import exporters


class Config(object):

    def __new__(cls, *args, **kwds):
        # singleton: create instance and store it as a class member
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        cls.__it__ = it = object.__new__(cls)
        it.init(*args, **kwds)
        return it

    def init(self):
        default_backend = exporters.LoggingExportBackend()
        # TODO: remove this
        self.export_backends = [default_backend, ]

    def add_export_backend(self, backend_cls, *args, **kwargs):
        backend = backend_cls(*args, **kwargs)
        self.export_backends.append(backend)
