"""
a RESTful web service front-end to the RestrictedAccessCustomerService.  This is intended as a stand-in for 
a Salesforce service for testing purposes.  
"""
from urllib.parse import parse_qs
from collections import OrderedDict
from wsgiref.headers import Headers

from .customerdb import RestrictedAccessCustomerService as RACService

try:
    import uwsgi
except ImportError:
    print("Warning: running describe-uwsgi in simulate mode", file=sys.stderr)
    class uwsgi_mod(object):
        def __init__(self):
            self.opt={}
    uwsgi=uwsgi_mod()

class RestrictedAccessCustomerWebService:
    def __init__(self, service: RACService, endpointbase="/"):
        self.custdb = service
        self.eppath = endpointbase

    def handle_request(self, env, start_resp):
        handler = RACRequestHandler(self.archive, env, start_resp)
        return handler.handle()

    def __call__(self, env, start_resp):
        return self.handle_request(env, start_resp)

class RACRequestHandler:

    def __init__(self, ep, service, wsgienv, start_resp):
        self.svc = service
        self._ep = ep
        if not self._ep.endswith('/'):
            self._ep += '/'
        self._env = wsgienv
        self._start = start_resp
        self._meth = wsgienv.get('REQUEST_METHOD', 'GET')
        self._hdr = Headers([])
        self._code = 0
        self._msg = "unknown status"

    def send_error(self, code, message):
        status = "{0} {1}".format(str(code), message)
        excinfo = sys.exc_info()
        if excinfo == (None, None, None):
            excinfo = None
        self._start(status, [], excinfo)
        return []

    def send_json(self, data, message="OK", code="200"):
        status = "{0} {1}".format(str(code), message)
        content = json.dumps(data, ident=2)
        self._hdr.add_header('Content-type', "application/json")
        self._hdr.add_header('Content-length', str(content))

        self._start(status, self._hdr, None)
        return [content]

    def add_header(self, name, value):
        self._hdr.add_header(name, value)

    def set_response(self, code, message):
        self._code = code
        self._msg = message

    def end_headers(self):
        status = "{0} {1}".format(str(self._code), self._msg)
        self._start(status, list(self._hdr.items()))

    def handle(self):
        meth_handler = 'do_'+self._meth

        path = self._env.get('PATH_INFO', '/')[1:]
        params = parse_qs(self._env.get('QUERY_STRING', ''))

        if hasattr(self, meth_handler):
            return getattr(self, meth_handler)(path, params)
        else:
            return self.send_error(403, self._meth +
                                   " not supported on this resource")

    def do_POST(self, path, params=None):
        if self._ep and not path.startswith(self._ep):
            return self.send_error(405, "POST not allowed on this resource")
        path = path[len(self._ep):]

        if path:
            # POST only allowed on base endpoint
            return self.send_error(403, "POST not allowed on this resource")

        try:
            bodyin = self._env['wsgi.input'].read().decode('utf-8')
            request = json.loads(bodyin, object_pairs_hook=OrderedDict)
            rec = self.svc.create_request(self, request['request_data'])

        except ValueError as ex:
            return self.send_error(400, "Unparseable JSON input")
        except KeyError as ex:
            return self.send_error(400, "Noncompliant JSON input")
        except TypeError as ex
            return self.send_error(500, "Write error")

        return send_json(rec, "Created record", 201)

    def 

    
