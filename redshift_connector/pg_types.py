from json import dumps


class PGType():
    def __init__(self, value):
        self.value = value

    def encode(self, encoding):
        return str(self.value).encode(encoding)


class PGEnum(PGType):
    def __init__(self, value):
        if isinstance(value, str):
            self.value = value
        else:
            self.value = value.value


class PGJson(PGType):
    def encode(self, encoding):
        return dumps(self.value).encode(encoding)


class PGJsonb(PGType):
    def encode(self, encoding):
        return dumps(self.value).encode(encoding)


class PGTsvector(PGType):
    pass


class PGVarchar(str):
    pass


class PGText(str):
    pass
