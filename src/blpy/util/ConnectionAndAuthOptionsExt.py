from ..blpapi_import_helper import blpapi
from .ConnectionAndAuthOptions import *
"""
This module extends the ConnectionAndAuthOptions class from the
ConnectionAndAuthOptions.py module in original blpapi examples. It adds the 
setAuthOptions method which allows the user to set the authentication options 
for the connection.
The method takes the following arguments:
    auth_type:  The type of authentication to use.  Valid values are:
        'none':  No authentication
        'user':  User authentication
        'app':   Application authentication
        'dir':   Active Directory authentication
        'manual':Manual authentication
        **kwargs:  The keyword arguments depend on the value of auth_type.
            For 'user' and 'dir' the keyword argument is 'auth_user' which
            is an instance of blpapi.AuthUser.
            For 'app' the keyword argument is 'app_name' which is a string
            containing the application name.
            For 'manual' the keyword arguments are 'ip' and 'user_id' which
            are the IP address and user ID to use for authentication.
            For 'none' there are no keyword arguments.
"""

def setAuthOptions(options,auth_type='none',**kwargs):
    """Set the authentication options for the connection."""
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
    setattr(options,'sessionIdentityAuthOptions',auth_options)

def setSessionOptions(options):
    """set session options"""
    sessionOptions=createSessionOptions(options)
    sessionOptions.setAutoRestartOnDisconnection(options.auto_restart)
    sessionOptions.setMaxEventQueueSize(options.queue_size)
    return sessionOptions