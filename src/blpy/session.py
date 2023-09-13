from .blpapi_import_helper import blpapi
from argparse import Namespace
import logging

class Session():
    global logger
    DEFAULT_QUEUE_SIZE=7500
    def __init__(self,**kwargs:any) -> None:
        


    def set_session_options(self):
        self.session_options = blpapi.SessionOptions()
        self.session_options.setServerAddress(self.host,self.port,0)
        # TODO: support multi hosts
        self.session_options.setSessionIdentityOptions(self.auth_options)
        self.session_options.setAutoRestartOnDisconnection(True)
        
        # default use an eventQueue
