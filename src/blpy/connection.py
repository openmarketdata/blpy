from .blpapi_import_helper import blpapi
from .util.ConnectionAndAuthOptionsExt import (
    setSessionOptions,
    setAuthOptions)
from argparse import Namespace
from collections import namedtuple
import multiprocessing
import logging
"""
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)-8s %(name)-12s %(message)s',
)
logger = logging.getLogger(__name__)
"""

class Connection():
    """Connection options generated from kwargs, one session per connection"""
    def __init__(self, **kwargs: any) -> None:
        self.logger=kwargs.pop('logger',logging.getLogger(__name__))
        # default to 127.0.0.1:8194
        defaults={'hosts':[namedtuple('HostPort', ['host', 'port'])
                                                  ('127.0.0.1',8194)],
                  'services':['//blp/refdata','//blp/mktdata'],
                  'queue_size': 7500,
                  'auto_restart':True,
                  # remote and tls options
                  'remote':0,
                  'tls_client_credentials':None,
                  'tls_client_credentials_password':None,
                  'tls_trust_material':None,
                  'read_certificate_files':None}
        defaults.update(kwargs)
        self.options=Namespace(**defaults)
        components={'event_queue':blpapi.EventQueue(),
                    'services':{},
                    'session':None}
        self.meta=Namespace(**components)
        # require auth options set when create Conneciton object
        setAuthOptions(self.options,**kwargs)
        # TODO: TLS, ZFP Options
        # set session options 
        sessionOptions = setSessionOptions(self.options)
        # create and start session
        session = blpapi.Session(sessionOptions)
        sessionOptions.destroy()
        if not session.start():
            raise RuntimeError('Failed to start session')
        self.meta.session=session
        # wait and process session status events
        self.retrieve_events('SESSION',end_event=[blpapi.Event.TIMEOUT])
        # open services
        self._open_services()
        # wait and process service status events
        self.retrieve_events('SESSION',end_event=[blpapi.Event.TIMEOUT])

    def _open_services(self):
        for service in self.options.services:
            if not self.meta.session.openService(service):
                raise RuntimeError('Failed to open service:'+service)
            self.logger.info('Opened service:'+service)
            self.meta.services[service]=self.meta.session.getService(service)
            self.logger.debug('Service:'+service+' has operations:')
            """
            for operation in self.meta.services[service].operations():
                self.logger.debug(operation.name())
                self.logger.debug(operation.description())
                self.logger.debug(operation.requestDefinition())
                self.logger.debug(operation.numResponseDefinitions())
                self.logger.debug(operation.numEventDefinitions())
                self.logger.debug(operation.numEventTemplates())
                self.logger.debug(operation.numServiceExceptions())
                self.logger.debug(operation.numServiceStatuses())
                self.meta
                self._gen_requests(operation)
            """

    def _get_operations(self,service):
        operations=dict()
        for operation in service.operations():
            operation.name()

    def _gen_requests():
        pass

    def _decode_schema_element(self,schema_element):
        schema=dict()
        for el in schema_element:
            schema_type=el.typeDefinition()
            schema_type_name=schema_type.name()
            schema_element_name=el.name()
            schema[schema_element_name]=schema_type_name
        return schema
    


    def help(self):
        """Return options help"""
        pass
    
    def retrieve_events(self,container,timeout=500,end_event=[blpapi.Event.TIMEOUT]):
        DONE=False
        events=[]
        while (not DONE):
            match container:
                case 'SESSION':
                    event=self.meta.session.nextEvent(timeout)
                case 'EVENT_QUEUE':
                    event=self.meta.event_queue.nextEvent(timeout)
                case _:
                    raise ValueError('Invalid container:'+container)
            event_type=event.eventType()
            events.append(self._process_event(event,self.meta.session))
            if event_type in end_event:
                DONE=True
        return events

    def _process_event(self,event,session):
        event_type=event.eventType()
        if event_type in [blpapi.Event.UNKNOWN,blpapi.Event.TIMEOUT]:
            self.logger.debug('Receiving event_type:'+str(event_type))
        messages=[]
        for message in event:
            message_type=message.messageType()
            if message_type in [blpapi.Names.SLOW_CONSUMER_WARNING,
                                blpapi.Names.SLOW_CONSUMER_WARNING_CLEARED]:
                self.logger.warn('Receiving message_type:'+str(message_type))
            else:
                self.logger.debug('Receiving message_type:'+str(message_type))
            # read doc in message.py for correlationsIds to support multi
            messages.append(message)
            match event_type:
                case blpapi.Event.SESSION_STATUS:
                    if message_type == blpapi.Names.SESSION_TERMINATED \
                    or message_type == blpapi.Names.SESSION_STARTUP_FAILURE:
                        self.logger.error(message_type)
                        session.stop()
                    return messages
                case blpapi.Event.SERVICE_STATUS:
                    if message_type == blpapi.Names.SERVICE_OPEN_FAILURE:
                        service_name=message.getElementAsString('serviceName')
                        self.logger.error('Fail to open:'+service_name)
                    return messages
        return messages



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