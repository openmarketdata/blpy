from .blpapi_import_helper import blpapi
from .util.ConnectionAndAuthOptions import createSessionOptions
from argparse import Namespace
from collections import namedtuple
import multiprocessing
import logging
"""
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)-8s %(name)-12s %(message)s',
)logger = logging.getLogger(__name__)
"""

class Connection():
    """Connection options generated from kwargs, one session per connection"""
    def __init__(self, **kwargs: any) -> None:
        self.logger=kwargs.pop('logger',logging.getLogger(__name__))
        # default to localhost DAPI
        defaults={'hosts':[namedtuple('HostPort', ['host', 'port'])
                                                  ('127.0.0.1',8194)],
                  'services':['//blp/refdata','//blp/mktdata'],
                  'tls_client_credentials':None,
                  'tls_client_credentials_password':None,
                  'tls_trust_material':None,
                  'read_certificate_files':None,
                  'remote':0,
                  'queue_size': 7500}
        defaults.update(kwargs)
        self.options=Namespace(**defaults)
        self.sessions=Namespace(**{'event_queue':blpapi.EventQueue()})
        # require auth options set when create Conneciton object
        self.set_auth_options(**kwargs)
        # TODO: TLS Options
        # TODO: ZFP Options
        # manually set session options or package into simplified request/subscribe calls
        sessionOptions = createSessionOptions(self.options)
        # create and start session
        session = blpapi.Session(sessionOptions)
        sessionOptions.destroy()
        if not session.start():
            raise RuntimeError('Failed to start session')
        # wait and process session status events
        while True:
            event = session.nextEvent(500)
            self.logger.info(event)
            if event.eventType() == blpapi.Event.SESSION_STATUS:
                if event.messageType() == blpapi.Name("SessionTerminated"):
                    break
            self.logger.debug(event)
        self.sessions.session=session

    def help(self):
        """Return options help"""
        pass
    
    def set_auth_options(self,auth_type='none',**kwargs):
        if auth_type in ['user','appuser']:
            auth_user = blpapi.AuthUser.createWithLogonName()
        if auth_type in ['app','appuser','manual']:
            if 'app_name' not in kwargs:
                raise KeyError('Missing app_name')
            app_name = kwargs.get('app_name')
        if auth_type == "dir":
            if 'dir_property' not in kwargs:
                raise KeyError('Missing dir_property')
            dir_property = kwargs.get('dir_property')
            auth_user = blpapi.AuthUser.createWithActiveDirectoryProperty(
                dir_property)
        if auth_type == 'manual':
            if 'ip' not in kwargs:
                raise KeyError('Missing ip')
            if 'user_id' not in kwargs:
                raise KeyError('Missing user_id')
            ip = kwargs.get('ip')
            user_id = kwargs.get('user_id')
            auth_user = blpapi.AuthUser.createWithManualOptions(user_id, ip)
        if auth_type == 'none':
            auth_options = None
        elif auth_type in ['user','dir']:
            auth_options = blpapi.AuthOptions.createWithUser(auth_user)
        elif auth_type == 'app':
            auth_options = blpapi.AuthOptions.createWithApp(app_name)
        elif auth_type == 'userapp':
            auth_options = blpapi.AuthOptions.createWithUserAndApp(auth_user,
                app_name)
        else:
            raise ValueError(f'Invalid auth_type:{auth_type}')
        setattr(self.options,'sessionIdentityAuthOptions',auth_options)


# class Connection:
#     """API connection with single session"""
#     DEFAULT_QUEUE_SIZE=7500
#     def __init__(self,host='127.0.0.1',port=8194,options={},logger=None):
#         self._host=host
#         self._port=port
#         self._options=options
#         self.logger=logger or logging.getLogger(__name__)
#         self.services={}
#         self._eq=blpapi.EventQueue()

#     def _defaultHandler(self,event,session):
#         """queue events"""
#         event_type=event.eventType()
#         self.logger.debug('event_type:'+str(event_type))
#         for msg in event:
#             message_type=msg.messageType()
#             self.logger.debug('message_type:'+str(message_type))            
#             if event_type == blpapi.Event.SESSION_STATUS:
#                 if message_type == blpapi.Names.SESSION_TERMINATED \
#                     or message_type == blpapi.Names.SESSION_STARTUP_FAILURE:
#                     self.logger.error(message_type)
#                     session.stop()                    
#             elif event_type == blpapi.Event.SERVICE_STATUS:
#                 if message_type == blpapi.Names.SERVICE_OPEN_FAILURE:
#                     service_name=msg.getElementAsString('serviceName')
#         return False

#     def _connect(self):
#         session_options=blpapi.SessionOptions()
#         session_options.setServerHost(self._host)
#         session_options.setServerPort(self._port)
#         session_options.setAutoRestartOnDisconnection(True)
#         cores=multiprocessing.cpu_count()
#         event_dispatcher=blpapi.EventDispatcher(cores)
#         event_dispatcher.start()
#         self.session=blpapi.Session(session_options
#                                    ,self._defaultHandler
#                                    ,event_dispatcher)
#         session_options.destroy()
#         if not self.session.start():
#             self.session.stop()
#             raise RuntimeError('Failed to start session')

#     def _getOperations(self,service):
#         operations={}

    
"""
pipe=blpy.Connection()

pipe.refdata

"""