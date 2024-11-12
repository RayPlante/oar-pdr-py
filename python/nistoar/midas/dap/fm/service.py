"""
An implementation of MIDAS-specific application layer of the Nextcloud-based file manager.  The 
application layer to Nextcloud provides functionality specific to use by MIDAS, namely:
  *  it creates spaces used organize and manage files associated with a MIDAS Digital Asset 
     Publication (DAP).
  *  it manages access permissions to a space to allow end users to upload files into it
  *  it manages the scanning of upload spaces, collecting metadata to be incorporated into 
     the DAP.

This implementation assumes that it has direct access to storage (for scanning purposes); however,
space manipulation is done through the Nextcloud's generic and WebDAV APIs using an administrative,
functional identity.  
"""
import os, logging
from logging import Logger
from copy import deepcopy
from pathlib import Path
from collections.abc import Mapping
from urllib.parse import urljoin

import requests

from .clients import NextcloudApi, FMWebDAVClient
from .exceptions import *
from nistoar.base.config import merge_config, ConfigurationException

class MIDASFileManagerService:
    """
    a service for managing file manager spaces on behalf of an end-user.  This class provides the 
    functionality of the MIDAS-specific application layer of the file manager.
    """

    def __init__(self, config: Mapping, log: Logger=None,
                 nccli: NextcloudApi=None, wdcli: FMWebDAVClient=None):
        """
        initialize the service

        :param dict          config:  the service configuration
        :param Logger           log:
        :param NextcloudApi   nccli:  the Nextcloud generic layer API client to use; if not provided,
                                      one will be constructed from ``config``.
        :param FMWebDAVClient wdcli:  the Nextcloud WebDAV client to use; if not provided, 
                                      one will be constructed from ``config``.
        """
        if not log:
            log = logging.getLogger('file-manager')
        self.log = log
        self.cfg = deepcopy(config)

        self._ncbase = self.cfg.get('nextcloud_base_url')
        if self._ncbase and not self._ncbase.endswith('/'):
            self._ncbase += '/'
        self._adminuser = self.cfg.get('admin_user')
        if not self._adminuser:
            raise ConfigurationException("MIDASFileManagerService: Missing config parameter: admin_user")
        self._root_dir = self.cfg.get("local_storage_root_dir")
        if self._root_dir:
            self._root_dir = Path(self._root_dir)
            if not self._root_dir.is_dir():
                raise ConfigurationException("local_storage_root_dir: does not exist as a directory")

        self._ncfilesurl = self.cfg.get('nextcloud_files_url')
        if not self._ncfilesurl:
            if not self._ncbase:
                raise ConfigurationException("MIDASFileManagerService: Missing config parameter: "+
                                             "nextcloud_files_url (or nextcloud_base_url)")
            self._ncfilesurl = "/".join([self.cfg['nextcloud_base_url'], "apps/files/files"])

        if not nccli:
            nccli = self.make_generic_layer_client()
        self.nccli = nccli
        if not wdcli:
            wdcli = self.make_webdav_client(nccli.base_url)
        self.wdcli = wdcli

    def make_webdav_client(self, generic_url: str=None, _override=None):
        """
        create an :py:class:`~nistoar.midas.dap.fm.clients.FMWebDAVClient` according to the 
        configuration provided to this class.

        :param str generic_url:  the base url for the Nextcloud generic layer API.  If provided, it 
                                 will be used to form the WebDAV client's default authentication endpoint 
        """
        cfg = deepcopy(self.cfg.get('webdav', {}))
        if _override:
            cfg = merge_config(_override, cfg)

        if not cfg.get('service_endpoint'):
            if not self._ncbase:
                raise ConfigurationException("Missing config parameter: webdav.service_endpoint")
            cfg['service_endpoint'] = urljoin(self._ncbase, f"remote.php/dav/files/{self._adminuser}")

        if not cfg.get('ca_bundle') and self.cfg.get('ca_bundle'):
            cfg['ca_bundle'] = self.cfg['ca_bundle']

        if not cfg.get('authentication'):
            cfg['authentication'] = deepcopy(self.cfg.get('authentication', {}))
        if not cfg['authentication'].get('client_auth_url') and generic_url:
            cfg['authentication']['client_auth_url'] = urljoin(generic_url, "auth")

        return FMWebDAVClient(cfg, self.log.getChild('webdav'))

    def make_generic_layer_client(self, _override=None):
        """
        create an :py:class:`~nistoar.midas.dap.fm.clients.FMWebDAVClient` according to the 
        configuration provided to this class.
        """
        cfg = deepcopy(self.cfg.get('webdav', {}))
        if _override:
            cfg = merge_config(_override, cfg)

        if not cfg.get('service_endpoint'):
            if not self._ncbase:
                raise ConfigurationException("Missing config parameter: webdav.service_endpoint")
            cfg['service_endpoint'] = urljoin(self._ncbase, f"api/genapi.php/")

        if not cfg.get('ca_bundle') and self.cfg.get('ca_bundle'):
            cfg['ca_bundle'] = self.cfg['ca_bundle']

        if not cfg.get('authentication'):
            cfg['authentication'] = deepcopy(self.cfg.get('authentication', {}))

        return NextcloudApi(cfg, self.log.getChild('generic'))

        

    def create_space_for(self, id: str, foruser: str):
        """
        create and set up the file space for a DAP record with the given ID.  

        :param str      id:  the identifier of the DAP being drafted that needs record space
        :param str foruser:  the identifier for the primary user of the space.  If this user does 
                             not known to nextcloud, it will be created.
        :rtype:  FMSpace
        """
        space = FMSpace(id, self)
        if self.space_exists(id):
            raise FileManagerOpConflict(f"{id}: space already exists")

        # create the user if necessary (may raise exception)
        self.ensure_user(foruser)

        # create the directories (may raise exception)
        self.wdcli.ensure_directory(space.root_folder)
        self.wdcli.ensure_directory(space.system_folder)
        self.wdcli.ensure_directory(space.uploads_folder)
        self.wdcli.ensure_directory(space.exclude_folder)
        self.wdcli.ensure_directory(space.trash_folder)

        # share space with user (may raise exception)
        if foruser != self._adminuser:
            space.set_permissions_for(space.root_folder, foruser, PERM_READ)
            # space.set_permissions_for(space.system_folder, userid, PERM_READ)
            space.set_permissions_for(space.uploads_folder, foruser, PERM_ALL)

        space.refresh_uploads_info()
        return space

    def space_exists(self, id: str):
        """
        return True if space for the given DAP record ID exists already
        """
        if self._root_dir:
            return (self._root_dir / id).exists()
        return self.wdcli.is_directory(id)

    def get_space(self, id: str):
        """
        return the space that has been set up for the DAP with the given record ID
        :rtype:  FMSpace
        """
        if not self.space_exists(id):
            raise FileManagerResourceNotFound(id)

        out = FMSpace(id, self)
        #  out.refresh_uploads_info()   # incurs an API call
        return out

    def delete_space(self, id: str):
        """
        remove the file space setup for a DAP record with the given ID
        :param str      id:  the identifier of the DAP being drafted whose space should be deleted
        """
        if not self.space_exists(id):
            raise FileManagerResourceNotFound(id)

        self.wdcli.delete_resource(id)

    def ensure_user(self, userid: str):
        """
        ensure that a user with the given id has been registered as a Nextcloud user, creating 
        the account if necessary.
        """
        if not self.nccli.is_user(userid):
            self.nccli.create_user(userid)

    def test(self):
        """
        test access to the Nextcloud API.
        """
        self.nccli.test()

PERM_NONE   = 0
PERM_READ   = 3
PERM_WRITE  = 7
PERM_DELETE = 15
PERM_SHARE  = 29
PERM_ALL    = 31

perm_name = {
    PERM_NONE:   "None",
    PERM_READ:   "Read",
    PERM_WRITE:  "Write",
    PERM_DELETE: "Delete",
    PERM_SHARE:  "Share",
    PERM_ALL:    "All"
}

class FMSpace:
    """
    an encapsulation of a file space in the file manager.  
    """
    trash_subfolder = "TRASH"
    exclude_subfolder = "EXCLUDE"

    PERM_NONE   = PERM_NONE
    PERM_READ   = PERM_READ
    PERM_WROTE  = PERM_WRITE
    PERM_DELETE = PERM_DELETE
    PERM_SHARE  = PERM_SHARE
    PERM_ALL    = PERM_ALL

    def __init__(self, id: str, fmsvc: MIDASFileManagerService, log: Logger=None):
        """
        initialize the view of the file space
        """
        self.svc = fmsvc
        self._id = id
        self._uploads_info = None
        if not log:
            log = self.svc.log.getChild(id)

    @property
    def id(self):
        """
        the identifier for the file space.  This usually matches the identifier for the DAP it is 
        associated with.
        """
        return self._id

    @property
    def root_folder(self):
        """
        the resource path to the root folder for the space.  This path is used to access the 
        folder via the WebDAV API.
        """
        return self.id

    @property
    def uploads_folder(self):
        """
        the resource path to the uploads folder.  This path is used to access the folder via 
        the WebDAV API.
        """
        return "/".join([self.root_folder, self.uploads_subfolder])

    @property
    def uploads_subfolder(self):
        """
        the resource path to the system folder for the space relative to the :py:prop:`root_folder`.
        """
        return f"{self.id}"

    @property
    def exclude_folder(self):
        """
        the resource path to the uploads folder.  This path is used to access the folder via 
        the WebDAV API.
        """
        return "/".join([self.uploads_folder, self.exclude_subfolder])

    @property
    def trash_folder(self):
        """
        the resource path to the uploads folder.  This path is used to access the folder via 
        the WebDAV API.
        """
        return "/".join([self.uploads_folder, self.trash_subfolder])

    @property
    def system_folder(self):
        """
        the resource path to the system folder for the space.  This path is used to access the 
        folder via the WebDAV API.
        """
        return "/".join([self.root_folder, self.system_subfolder])

    @property
    def system_subfolder(self):
        """
        the resource path to the system folder for the space relative to the :py:prop:`root_folder`.
        """
        return f"{self.id}-sys"

    def refresh_uploads_info(self):
        """
        use the WebDAV API to fetch and update the internally cached metadata about the uploads
        folder (including its Nextcloud file id).
        """
        self._uploads_info = self.get_resource_info(self.uploads_subfolder)

    def get_resource_info(self, resource: str):
        """
        return the Nextcloud metadata describing the specified resource with in the space.

        :param str resource:  the path to the resource relative to the space's root folder
        """
        out = self.svc.wdcli.get_resource_info('/'.join([self.root_folder, resource]))
        if out.get('fileid'):
            out['gui_url'] = self._make_gui_url(out['fileid'])
        return out

    def resource_exists(self, resource: str):
        """
        return True if the named resource exists in the space

        :param str resource:  the path to the resource relative to the space's root folder
        :rtype: bool
        """
        if self.svc._root_dir:
            path = os.sep.join(resource.split('/'))
            return (self.svc._rootdir / path).exists()

    def _make_gui_url(self, ncresid):
        return f"{self.svc._ncfilesurl}/{ncresid}?dir={self.uploads_folder}"

    def get_permissions_for(self, resource: str, userid: str):
        """
        return the permission level on the specified resource assigned to the given user.

        Nextcloud permissions reflect a level of access (as opposed to a set of access rights
        that can be assigned independently), where ``PERM_NONE`` enforces no access included 
        read, and ``PERM_ALL`` allows complete access.  

        :param str resource:  the path to the resource relative to the space's root folder
        :param str   userid:  the id of the user of interest
        :return:  the permission code 
                  :rtype: int
        """
        pdata = self.svc.nccli.get_user_permissions(resource)
        if not pdata.get('ocs'):
            if not self.resource_exists(resource):
                raise FileManagerResourceNotFound(resource)
            raise UnexpectedFileManagerResponse(f"Unexpected permissions query response: missing ocs property"+
                                                "\n  "+str(pdata))
        if not pdata['ocs'].get('data'):
            self.log.warning("Missing permission info for resource, %s", resource)
            return PERM_NONE

        for data in pdata['ocs']['data']:
            if data['share_with'] == userid:
                return data['permissions']

        return PERM_NONE
        

    def set_permissions_for(self, resource: str, userid: str, perm: int):
        """
        assign the permission level on the specified resource to the given user

        :param str resource:  the path to the resource relative to the space's root folder
        :param str   userid:  the id of the user of interest
        :param int     perm:  the permission level code to set
        """
        if perm not in perm_name:
            raise ValueError(f"perm: code not recognized: {perm}")

        self.svc.ensure_user(userid)
        self.svc.nccli.set_user_permissions(userid, perm, resource)
        

    def launch_scan(self, type: str = "def"):
        """
        start a scan of the contents of the space

        :param str type:  a label identifying the type of scan to launch.  The default 
                          is "def", indicating the default type.
        :return:  preliminary data resulting from the initial "fast scan" of the space.
                  This will include "scanid" which can be used to retrieve the scan 
                  results later.
                  :rtype: dict
        """
        pass

    def get_scan(self, scanid: str):
        """
        return the current results from the specified scan

        :param str scanid:  the unique ID assigned to the scan
        :return:  the data that has been collected thus far from the scan operation
                  This will include "is_complete" which will be False if the scan is 
                  still in progress.
                  :rtype: dict
        """
        pass

    def delete_scan(self, scanid: str):
        """
        stop a scan operation (if it is still running) and delete its artifacts from the 
        system folder.
        :param str scanid:  the unique ID assigned to the scan
        """
        pass







    # permissions

    # scan
        
