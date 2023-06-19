"""
Implementations of the presersvation task framework that are specific to the NIST Public Data Repository 
and how it handles preservation.
"""

import os, time, datetime, logging, json
from pathlib import Path
from collections import OrderedDict

from .. import framework as fw
from nistoar.pdr.preserve.bagit import BagBuilder, BagWriteError, BagItException
from nistoar.pdr.exceptions import ConfigurationException
import nistoar.pdr.preserve.bagit.utils as bagutils
import nistoar.id.versions as verutils
from nistoar.pdr.ingest import RMMIngestClient, DOIMintingClient
from nistoar.pdr.distrib import (BagDistribClient, RESTServiceClient,
                                 DistribResourceNotFound, DistribServiceException)

DEF_MBAG_VERSION = bagutils.DEF_MBAG_VERSION

class PDRBagFinalization(fw.AIPFinalization):
    """
    An iplementation of the AIP finalization step that applies some last-minute tweaks to the submitted 
    AIP bag before validation and serialization.  It also extracts key metadata products that will be 
    part of the final publishing step.  

    This implementation supports the following configuration parameters:
    :allow_replace:  allow an AIP to replace a previously preserved AIP with the same version number
                   (default: False).  Use with caution.
    :repo_access:  a configuration dictionary that describe access points to the public side of the 
                   repository.  This is used to interrogate details of previous publications of a
                   dataset.  
    :bag_builder:  a configuration dictionary for configuring the 
                   :py:class:`~nistoar.pdr.preserve.bagit.builder.BagBuilder` instance that will be 
                   used to make the final updates.
    :doi_minter:   a configuration dictionary for configuring the DOI minting client used to stage the 
                   DataCite record
    :ingester:     a configuration dictionary for configuring the ingest client used to stage the 
                   NERDm record
    """

    def __init__(self, config=None):
        """
        instantiate the finalization step
        :param dict config:  the configuration for this step; if not provided, defaults will apply.
        """
        if config is None:
            config = {}
        self.cfg = config

        if not self.cfg.get("repo_access", {}).get("distrib_service", {}).get("service_endpoint"):
            raise ConfigurationException("Missing required configuration: "+
                                         "repo_access.distrib_service.service_endpoint")

        icfg = self.cfg.get('ingest', {})
        self._ingester = None
        ingcfg = icfg.get('rmm')
        if ingcfg and ingcfg.get('service_endpoint'):
            self._ingester = RMMIngestClient(ingcfg)

        self._doiminter = None
        dmcfg = icfg.get('doi')
        if dmcfg and dmcfg.get('datacite_api'):
            self._doiminter = DOIMintingClient(dmcfg)


    def apply(self, statemgr: fw.PreservationStateManager):
        """
        apply the finalization step.  This implementation will give the original bag a new name.
        """
        log = statemgr.log.getChild("finalization")

        aipid = statemgr.aipid
        bagdir = Path(statemgr.get_original_aip())
        if bagdir is None:
            raise fw.AIPFinalizationException("Initial AIP is not set via state manager", aipid)
        if not bagdir.is_dir():
            raise fw.AIPFinalizationException(f"Initial AIP is not an existing directory: {bagdir}",
                                              aipid)

        statemgr.record_progress("Finalizing the AIP bag")

        # start by determining the sequence number; if this fails, we shouldn't go on
        repo = RepositoryAccess(self.cfg.get('repo_access'), log)
        lastver = None
        lastseq = -1
        try:
            latestbag = repo.latest_headbag(aipid)
            if latestbag:
                parts = bagutils.parse_bag_name(latestbag)
                lastver = verutils.OARVersion(parts[1])
                lastseq = int(parts[3])
        except ValueError as ex:
            raise fw.AIPFinalizationException("Unexpected error interpreting previously published AIP "+
                                              f"name: {str(latestbag)}: {str(ex)}", aipid)
        # except repoaccess error

        bldr = BagBuilder(str(bagdir.parents[0]), str(bagdir.name), self.cfg.get("bag_builder"),
                          statemgr.aipid, log)
        try:
            bldr.ensure_bagdir()
            bldr.record("Beginning preservation")

            # check version.  New version should already be set in bag.
            newver = bldr.bag.nerd_metadata_for("", True).get("version")
            if not newver:
                raise fw.AIPFinalizationException(f"{aipid}: Version not set in AIP")
            newver = verutils.OARVersion(newver)
            if newver <= lastver:
                if self.cfg.get("allow_replace", False):
                    if newver < lastver:
                        raise fw.AIPFinalizationException("f{aipid}: {newver}: Can't replace "+
                                                          f"version earlier than last version ({lastver})")
                    log.warning("Preservation set to replace previous published version: %s", newver)
                else:
                    raise fw.AIPFinalizationException(
                        "f{aipid}: {newver} AIP version already published; won't replace!"
                    )
                        

            # check sequence number
            newseq = lastseq + 1

            # rename the bag based on version/seq. #
            newname = self.form_bag_name(aipid, newseq, str(newver))
            newdir = os.path.join(bldr._pdir, newname)
            if os.path.exists(newdir):
                raise fw.AIPFinalizationException(f"{aipid}: Unable to rename input bag: "+
                                                  "destination name already exists!", aipid)
        except BagItException as ex:
            raise fw.AIPFinalizationException(f"{aipid}: Problem accessing input bag, {bagdir}: " +
                                              str(ex)) from ex

        if newname != bagdir.name:
            try:
                bldr.done()
                bagdir.rename(newdir)
                statemgr.set_finalized_aip(newdir)
            except OSError as ex:
                raise fw.AIPFinalizationException(f"{aipid}: Unable to rename {bagdir} to {newdir}: " +
                                                  str(ex)) from ex
            bagdir = Path(newdir)
            bldr = BagBuilder(str(bagdir.parents[0]), str(bagdir.name), self.cfg.get("bag_builder"),
                              statemgr.aipid, log)
            bldr.ensure_bagdir()

        # now finalize on newly renamed bag
        try:
            bldr.finalize_bag(self.cfg.get("bag_builder", {}).get("finalize", {}))
            bldr.done()
        except BagItException as ex:
            raise AIPFinalizationException("Failed to finalize AIP bag: "+str(ex)) from ex

        # stage NERDm record for publishing
        nerdm = bldr.bag.nerdm_record()
        if self._ingester:
            try:
                self._ingester.stage(nerdm, aipid)
            except Exception as ex:
                msg = f
                log.exception("Failure staging NERDm record for %s for ingest: %s", aipid, str(ex))
        else:
            log.warning("Ingester client not configured: archived records will not get loaded to repo")

        # stage DataCite record for DOI minting/updating
        if not self._doiminter:
            log.warning("DOI minting client not configured: archived records will not get submitted "+
                        "to DataCite")
        if 'doi' not in nerdm:
            log.warning("No DOI assigned to aip=%s; skipping Datacite submission", aipid)
        else:
            try:
                self._doiminter.stage(nerdm, name=aipid)
            except Exception as ex:
                log.exception("Failure staging DataCite record for %s for DOI minting/updating: %s",
                              aipid, str(ex))

        statemgr.mark_completed(statemgr.FINALIZED, "AIP bag finalization completed")


    def revert(self, statemgr: fw.PreservationStateManager):
        log = statemgr.log.getChild("finalization").getChild("revert")
        didstuff = False
        finalized = statemgr.get_finalized_aip()
        if finalized and os.path.exists(finalized):
            statemgr.set_finalized_aip(None)
            try:
                orig = statemgr.get_original_aip()
                if orig and os.path.exists(orig):
                    log.warning("Original AIP (unexpectedly) exists; deleting previously finalized version")
                    if os.path.is_dir(finalized):
                        shutil.rmtree(finalized)
                    else:
                        log.warning("Finalized AIP is unexpectedly a file; removing anyway")
                        os.unlink(finalized)

                else:
                    os.rename(finalized, orig)

                didstuff = True
                
            except OSError as ex:
                raise AIPFinalizationException("Failed to revert finalized bag back to original: "+str(ex)) \
                    from ex

        if self._ingester and self._ingester.is_staged(statemgr.aipid):
            self._ingester.clear(statemgr.aipid)
            didstuff = True
        if self._doiminter and self._doiminter.is_staged(statemgr.aipid):
            self._doiminter.clear(statemgr.aipid)
            didstuff = True

        return didstuff
            
                

    # apply:
    #  *  determine sequence number
    #  *  finalize log, provenance info
    #  *  finalize baginfo.txt
    #  *  rename bag
    #  *  stage nerdm record
    #  *  stage datacite record

    # revert:
    #  *  unstage nerdm, datacite records
    #  *  rename bag back to original name

    # cleanup:
    #  *  ?

    def form_bag_name(self, aipid, bagseq=0, aipver="1.0"):
        """
        return the name to use for the working bag directory.  According to the
        NIST BagIt Profile, preservation bag names will follow the format
        AIPID.AIPVER.mbagMBVER-SEQ

        :param str  aipid:   the AIP identifier for the dataset
        :param int  bagseq:  the multibag sequence number to assign (default: 0)
        :param str  aipver:  the dataset's release version string.  (default: 1.0)
        """
        fmt = self.cfg.get('bag_name_format')
        bver = self.cfg.get('mbag_version', DEF_MBAG_VERSION)
        return bagutils.form_bag_name(aipid, bagseq, aipver, bver, namefmt=fmt)

class RepositoryAccess:
    """
    an interface to the repositories APIs as well as local caches to determine the existance of 
    previously published resources. 

    This class looks for the following configuration parameters:

    :distrib_service:   a description of the repository's distribution service, used for interrogating
                        previously published datasets via their AIP bags.  The dictionary value
                        requires only one sub-parameter, ``service_endpoint``, that gives the REST
                        service's base URL.
    :metadata_service:  a description of the repository's metadata service, used for interrogating
                        previously published datasets via their NERDm records.  (This is used to 
                        find publications that did not previously get published via the preservation
                        service and which, therefore, has no associated bags.)  The dictionary value
                        requires only one sub-parameter, ``service_endpoint``, that gives the REST
                        service's base URL.
    :headbag_cache:     a local directory that is use for caching head bags that have gone through the
                        preservation service.
    :store_dir:         a local directory providing the gateway to long-term storage of public AIP bags. 
    :restricted_store_dir:  a local directory providing the gateway to long-term storage of restricted-
                        access AIP bags.
    """

    def __init__(self, config, log=None):
        self.cfg = config
        self.log = log
        if not self.log:
            self.log = logging.getLogger("quiet")
            self.log.setLevel(logging.CRITICAL+10)  # silent

        self.distrib = None
        ep = self.cfg.get("distrib_service",{}).get("service_endpoint")
        if ep:
            self.distrib = RESTServiceClient(ep)
        

    def latest_headbag(self, aipid, include_in_process=True):
        """
        return the name of the latest headbag available for the dataset with the given AIP identifier
        :param str aipid:                the identifier of AIP of interest
        :param bool include_in_process:  if True (default), this method will also consult local caches 
                                         for bags that may still be going through the preservation 
                                         process.  If False, the answer will return the bag that has 
                                         fully completed the preservation (and ingest) process.  
        :raises DistribServiceException:  when a failure occurs while consulting the distribution service
        """
        candidates = []
        if self.distrib:
            distrib = BagDistribClient(aipid, self.distrib)
            try:
                candidates.extend([ distrib.head_for_version() ])
            except DistribResourceNotFound as ex:
                pass

        elif not include_in_process:
            raise DistribServiceException("Unable to determine latest head bag: "
                                          "No distribution service configured")
        else:
            self.log.warning("headbag queries are less accurate without access to distribution service")

        if include_in_process:
            locals = (
                self.cfg.get('store_dir'),
                self.cfg.get('restricted_store_dir'),
                self.cfg.get('headbag_cache')
            )

            for loc in locals:
                if loc and os.path.isdir(loc):
                    bags = [f for f in os.listdir(loc)
                              if f.startswith(aipid+".") and bagutils.is_legal_bag_name(f)]
                    if bags:
                        candidates.append(bagutils.find_latest_head_bag(bags))

        if not candidates:
            return None

        return bagutils.find_latest_head_bag(candidates)

                    
            

        
