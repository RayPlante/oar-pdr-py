"""
This module proivdes the base interface and infrastructure for the notification 
service.

The notification framework has three basic classes:

   *  ChannelService -- a service that can send notifications via some 
        "channel"--i.e. distribution mechanism (like email or text message).
        Implementations of this base class specialize for the particular
        mechanism.
   *  NotificationTarget -- a logical destination for a notifications.  A 
        target is configured with a name that represents a particular role 
        or responsibility for receiving messages of a particular concern 
        (like "operator" or "webmaster").  Implementations of this base 
        class are specialized for a particular channel, and instances are 
        attached to the appropriate ChannelService.
   *  Notice -- a container for the notification.  

A notification is distributed to configured recipients by creating a Notice
and sending it to a NotificationTarget.  The NotificationTarget formats the 
message appropriately and sends it to the ChannelService with the configured 
set of recipients.  The ChannelService sends the message using its particular 
distribution mechanism.  
"""
from datetime import datetime
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import Mapping, OrderedDict
import json

from ... import pdr as _pdr
from ...pdr import exceptions as _pdrexc

class NotificationTarget(object, metaclass=ABCMeta):
    """
    a base class for a destination to send notifications to.
    """

    def __init__(self, service, config=None, name=None, fullname=None):
        """
        initialize the target.  This implementation sets the name and fullname
        which are normally extracted from the given configuration.  Values 
        provided for these as arguments are default values that are used if 
        not set in the configuration.

        :param service ChannelService: the service to associate with this 
                         destination.  The implementation will check to ensure
                         that the service is of the right type for this target.
        :param config dict:  a configuration that can include 'name' and 
                         'fullname' properties (with the same meaning as 
                         below) as well as implementation specific properties.
        :param name str: the unique label that can be used to identify this 
                         target
        :param fullname str: a human-friendly name to use for identifying the 
                         target of the notification.  This can be a person's 
                         name, but more often it is a name for a functional 
                         group.
        """
        if config is None:
            config = {}
        if not isinstance(config, Mapping):
            raise TypeError("configuration must be a dictionary-like object")
        self._cfg = config
        self.service = service
        self.name = self._cfg.get('name', name)
        self.fullname = self._cfg.get('fullname', fullname)

    @abstractmethod
    def send_notice(self, notice, targetname=None, fullname=None):
        """
        send the given notice to this notification target.  The target is 
        not guaranteed to use either the targetname or fullname inputs.

        :param notice Notice:  the notification to send to the target
        :param targetname str: a name to use as the name of the target, 
                               over-riding this target's configured name.
        :param fullname str: a name to use as the full name of the target, 
                               over-riding this target's configured fullname.
        """
        pass

class ChannelService(object, metaclass=ABCMeta):
    """
    a base class for a service that can send notifications to 
    NotificationTargets of its type.
    """

    def __init__(self, config):
        """
        initialize the service with a configuration
        """
        if not config:
            config = {}
        self.cfg = config

class Notice(object):
    """
    a notification message that should be sent to one or more targets.  
    """

    def __init__(self, type, title, desc=None, origin=None, issued=None, 
                 formatted=False, **mdata):
        """
        create the Notice with metadata.  The extra keywords can be arbitrary 
        data that can be added to the out-going message.  

        The information that is actually included in the delivered 
        notification is dependent on the NotificationChannel used.  Some 
        mechanisms may keep the content short and opt to include only minimal 
        information.

        The notice description can either be a string or a list of strings.  
        In the latter case, each element will be treated as a separate 
        paragraph when rendering it in a particular channel (like an email).
        That channel may apply special formatting to each element for display
        purposes, such as inserting newline characters to ensure lines that do
        not exceeed a particular width--unless, that is, if formatted=True.  
        This says that special formatting has already been applied and further 
        formatting would corrupt it; thus, formatted=True turns off downstream
        formatting.  

        :param type str:   a label indicating the type or severity of the 
                           notification.
        :param title str:  a brief title or subject for the notification
        :parma desc str or list of str:  a longer description of the 
                           reason for the notification.  (See also above 
                           discussion about description.)
        :param origin str: the name of the software component that is issuing 
                           the notification.
        :param issued str: a formatted date/time string to include; if not 
                           provided, one will be set from the current time.
        :param formatted bool:  If False, the description is unformatted for 
                           line width; in this case, the description may 
                           get formatted for certain outputs like an email.
                           (See above discussion about the description.)
                           If True, the description has been pre-formatted
                           (or otherwise should not be formatted) for display.
        :param mdata dict: arbitrary metadata to (optionally) include
        """
        self.type = type
        self.title = title
        self.description = desc
        self.doformat = not formatted
        self.origin = origin
        if not issued:
            issued = self.now()
        self._issued = issued
        self._md = mdata.copy()
        if 'platform' not in self._md:
            self._md['platform'] = _pdr.platform_profile

    @property
    def issued(self):
        return self._issued

    @property
    def metadata(self):
        return self._md

    def now(self):
        """
        Return a formatted time-stamp representing the current time
        """
        return datetime.now().strftime("%a, %d %b %Y %H:%M:%S")

    def to_json(self, pretty=True):
        """
        export this notice into a JSON object
        """
        out = OrderedDict([
            ("type", self.type),
            ("title", self.title),
            ("issued", self.issued)
        ])
        if self.description:
            out['description'] = self.description
        if self.origin:
            out['origin'] = self.origin
        if self.origin:
            out['metadata'] = self.metadata

        fmt = {}
        if pretty:
            fmt['indent'] = 2
            fmt['separators'] = (', ', ': ')
        return json.dumps(out, **fmt)

    @classmethod
    def from_json(cls, data):
        """
        turn the JSON data into a Notice object
        """
        if isinstance(data, str):
            data = json.loads(data)
        elif hasattr(data, 'read'):
            data = json.load(data)
        if not isinstance(data, Mapping):
            raise TypeError("Notice.from_json(): arg is not JSON data")

        mdata = data.get("metadata", {})
        return Notice(data.get('type'), data.get('title'),
                      data.get('description'), data.get('origin'),
                      data.get('issued'), **mdata)
            
class NotificationError(_pdrexc.PDRException):
    """
    a class indicating that the PDR system or environment is in 
    an uncorrectable state preventing proper processing
    """
    pass

        
