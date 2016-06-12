#!-*- encoding=utf-8

class MegNoriBaseException(Exception):
    pass

class NoAvaliableVolumeError(MegNoriBaseException):
    pass

class PutFileException(MegNoriBaseException):
    pass

class GetFileException(MegNoriBaseException):
    pass
