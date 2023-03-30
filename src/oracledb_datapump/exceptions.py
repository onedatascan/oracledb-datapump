import oracledb


class InvalidObjectType(ValueError):
    def __init__(self, object_type: str, valid_types: list[str]):
        self.message = (
            "Invalid object type {} provided! Valid types: {}".format(
                object_type, valid_types
            ),
        )
        super().__init__(self.message)


class UsageError(Exception):
    pass


class JobNotFound(Exception):
    pass


class FileNotPrepared(Exception):
    pass


class DataPumpCompletedWithErrors(Exception):
    pass


class BadRequest(Exception):
    pass


class Unsupported(Exception):
    pass


DatabaseError = oracledb.DatabaseError
