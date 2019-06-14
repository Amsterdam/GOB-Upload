import gc


class ActiveGarbageCollection:

    def __init__(self, title):
        assert gc.isenabled(), "Garbage collection should be enabled"
        self.title = title

    def __enter__(self):
        self._collect("start")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._collect("completion")

    def _collect(self, step):
        n = gc.collect()
        if n > 0:
            print(f"{self.title}: freed {n} unreachable objects on {step}")
