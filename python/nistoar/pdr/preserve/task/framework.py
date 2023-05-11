"""
This module provides the base classes that define a framework for managing a processing task.
The Preservation Task Framework provides a model for how the process for preserving products is organized.  
The process can be a long-running one which can experience various errors along the way.  This framework
allows one to execute the process in a variety ways--such as, synchronously via the command line or 
asynchronously via a web service.  If the process fails along the way, the framework can allow the process
to be resumed at the right after the failure conditions have been corrected without starting over.  

The preservation process operates on the PDR's notion of an Archive Information Package (AIP), which takes 
the form of a BagIt bag.  The process is defined by the following steps, each represented by a pluggable 
interface:
  1. :py:class:`AIPFinalization` -- setting up the preservation process, which can include last-minute 
     tweaks to the content of the bag.  
  2. :py:class:`AIPValidation` -- running a series of validation checks to ensure that the AIP meets the 
     requirements for completeness.
  3. :py:class:`AIPSerialization` -- converting the AIP into one or more archivable files
  4. :py:class:`AIPArchiving` -- committing the serialized bag files to long-term storage
  5. :py:class:`AIPPublication` -- releasing the AIP to external systems, namely to a repository system 
     through which the AIP can be accessed.  

These steps are managed via an instance of the :py:class:`PreservationTask` class.  
"""
from collections.abc import Mapping
from abc import ABCMeta, abstractmethod, abstractproperty
import logging
from logging import Logger
from typing import List

UNSTARTED_PROGRESS = "waiting to start preservation"

class PreservationStepsAware:
    UNSTARTED  =  0     # preservation of the AIP has not yet been started
    STARTED    =  1     # preservation finalization has started
    FINALIZED  =  2     # the AIP has been finalized for preservation
    VALIDATED  =  4     # the AIP has been found to be valid and ready for preservation
    SERIALIZED =  8     # the AIP has been serialized
    SUBMITTED  = 16     # the serialized AIP files have been submitted for migration to long-term storage
    ARCHIVED   = 32     # the migration to long-term storage is complete
    PUBLISHED  = 64     # the AIP has been released to external services

    _last_step = PUBLISHED
    _all_steps = (_last_step << 1) - 1
    _step_label = {
        UNSTARTED  : "unstarted",
        STARTED    : "started",
        FINALIZED  : "finalized",
        VALIDATED  : "validated",
        SERIALIZED : "serialized",
        SUBMITTED  : "submitted to archive",
        ARCHIVED   : "archived",
        PUBLISHED  : "published"
    }

    def _last_step_in(cls, state):
        step = cls._last_step
        while step > 0 and step & state == 0:
            step >>= 1
        return step

    def _label_for_step(cls, state):
        return cls._step_label[cls._last_step_in(state)]
        
class PreservationStateManager(PreservationStepsAware, metaclass=ABCMeta):
    """
    a class that tracks the state of the preservation process of an AIP.  It not only encapsulates 
    the AIP being preserved but also the mechanisms for persisting the state of the preservation,
    including how intermediate products are managed.  

    An instance of this class is used to coordinate the different steps in a :py:class:`PreservationTask`
    by passing information between steps.  Some of the information includes storage locations of 
    data or "directories" where data can be written to.  Typically, these locations are interpreted as 
    a filesystem path; however, an implemenation may express them as URIs (which must be properly formatted
    as a URI).  Such URI-based implementations will need to be paired with :py:class:`PreservationTask`
    implementations that can utilized such locations.

    .. seealso:: implementations :py:mod:`~nistoar.pdr.preserve.task.state`
    """

    def __init__(self, aipid: str, config: Mapping, logger: Logger=None):
        """
        instantiate the state manager.  
        :param str      aipid:  the identifier of the AIP to preserve.  The ``config`` must 
                                indicate where this AIP is located.
        :param Mapping config:  the configuration that controls the behavior of the manager.
                                The expected configuration properties is implementation-specific.
        :param Logger  logger:  the logger to use during preservation.
        """
        if not logger:
            logger = logging.getLogger("preserve").getChild(aipid)
        self._log = logger
        self.cfg = config
        self._aipid = aipid
        self._completed = self.UNSTARTED   # TODO: hold this in a dictionary with other state?
        self._keepfresh = self.cfg.get("keep_fresh", True)

    @property
    def aipid(self) -> str:
        """
        the identifier for the AIP being preserved.
        """
        return self._aipid

    @property
    def log(self) -> Logger:
        """
        the Logger that can be used to record message from the preservation process
        """
        return self._log

    @abstractproperty
    def message(self) -> str:
        """
        a message describing current progress in the preservation process.  This can be more 
        fine-grained than the label returned by :py:meth:`completed`.  
        """
        raise NotImplementedError()

    @abstractproperty
    def steps_completed(self) -> int:
        """
        a bit array that indicates the preservation steps that have been successfully applied
        """
        raise NotImplementedError()

    @property
    def completed(self) -> str:
        """
        a label indicating the latest completed stage of preservation
        """
        return self._label_for_step(self.steps_completed)

    @abstractmethod
    def mark_completed(self, step: int, message=None):
        """
        indicate that the given step has been completed.  This is intended to be called by a 
        :py:class:`PreservationStep` when it successfully completes.
        :param int step:  the :py:class:`PreservationCompleted` constant indicating the step that has 
                          been completed.  Multple steps can be so marked by OR-ing them together.  
        :param str message:  If provided, update the a progress message with this string
        """
        raise NotImplementedError()

    @abstractmethod
    def _load(self):
        """
        load the state from its persistent stroage
        """
        raise NotImplementedError()

    @abstractmethod
    def _cache(self):
        """
        load the state from its persistent stroage
        """
        raise NotImplementedError()

    @abstractmethod
    def get_original_aip(self) -> str:
        """
        return the original location of the submitted AIP.  Typically, the value represents a bag
        root directory; however, in general, it could be a URI interpreted in an implementation-specific
        way.  The AIP's existance at that location depends on the state of the preservation process; it 
        is not guaranteed to exist at this location at the time this function is called.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_finalized_aip(self) -> str:
        """
        return the location of the finalized AIP--i.e. the location of the AIP that is 
        has been (or will be) finalized prior to validation.  Typically, the value represents a bag
        root directory; however, in general, it could be a URI interpreted in an implementation-specific
        way.  The AIP's existance at that location depends on the state of the preservation process; it 
        is not guaranteed to exist at this location at the time this function is called.
        :return:  the location of the AIP after the finalization step has been applied, or None if it 
                  is not known, yet. 
        """
        raise NotImplementedError()

    @abstractmethod
    def set_finalized_aip(self, loc):
        """
        set the location of the finalized AIP--i.e. the location of the AIP that is 
        has been (or will be) finalized prior to validation.  Typically, the value represents a bag
        root directory; however, in general, it could be a URI interpreted in an implementation-specific
        way.  The AIP's existance at that location depends on the state of the preservation process; it 
        is not required to exist at this location at the time this function is called.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_stage_dir(self) -> str:
        """
        return the directory (or other URI-based location) where serialized AIP files will be staged to 
        during the serialization process.  
        """
        raise NotImplementedError()

    @abstractmethod
    def set_serialized_files(self, aipfiles: List[str]):
        """
        Set the list of files that were (or will be) created from serializing the AIP.

        This is typically called by a AIPSerialization implementation to report where it wrote (or 
        will write) its files.  The AIPArchiving step can then use :py:meth:`get_serialized_aip_files`
        to get the list of files to archive.  This should be a complete list--not a partial one.
        The files are not required to exist at these locations at the time this function is called.  

        :param list aipfiles:  a list of paths (or URIs) pointing to all of the serialized AIP files
                               resulting from the serialization step.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_serialized_files(self) -> List[str]:
        """
        Return the list of files that were (or will be) created from serializing the AIP.  The files'
        existance at these locations depends on the state of the preservation process; they are not 
        guaranteed to all exist at the time it is called.

        This is typically called by a AIPArchiving implementation to get the list of files to archive.  

        :return:  a list of string paths (or URIs) pointing to the complete list of serialized AIP files
                  that resulting from the serialization step.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_state_property(self, name: str, default=None): 
        """
        get an arbitrary property describing some part of the state of the preservation process.  
        This allows two steps in the process (which need not be sequential) to coordinate their 
        behavior. 
        """
        raise NotImplementedError()

    @abstractmethod
    def set_state_property(self, name: str, value):
        """
        set (and persist) an arbitrary property describing some part of the state of the preservation 
        process.  This allows two steps in the process (which need not be sequential) to coordinate 
        their behavior. 
        """
        raise NotImplementedError()

    @abstractmethod
    def record_progress(self, message: str):
        """
        Update the progress message
        """
        raise NotImplementedError()


class PreservationStep(metaclass=ABCMeta):
    """
    an abstract interface for one step in the preservation process.  Specific steps are typically
    derived from this interface, from which particular implementations of those steps are derived.

    To apply a step, an instance is passed via its functions a PreservationStateManager instance
    which encapsulates the AIP being preserved as well as the state of its progress
    """
    def run(self, statemgr: PreservationStateManager, ignore_cleanup_error: bool=True):
        """
        Apply the preservation processing step.  This is normally done by first calling 
        :py:meth:`revert` to clean up from a previously (failed) attempt, then 
        :py:meth:`apply`, followed by (if :py:meth:`apply` is successful) :py:meth:`clean_up`.  
        Consequently, one should expect that the outcome from any previous runs of this step 
        will be undone or otherwise obliterated.

        :param PreservationStateManager statemgr:  the state manager that encapsulates the AIP 
                                       being preserved and the state of its progress.
        :param bool ignore_cleanup_error:  if True (default), any errors that occur while calling 
                                       :py:meth:`clean_up` will be caught.  Regardless of this 
                                       value, the exception will be logged as a warning.  
        :raise PreservationException:  if an error occurred preventing the completion of this
                                       step.
        """
        self.revert(statemgr)
        self.apply(statemgr)
        self.report_completed(statemgr)
        try:
            self.clean_up(statemgr)
        except PreservationException as ex:
            self._report_cleanup_failure(statemgr, ex)
            if not ignore_cleanup_error:
                raise

    @abstractmethod
    def apply(self, stagemgr: PreservationStateManager):
        """
        Apply this preservation step to the target AIP.  One should expect that the outcome from any 
        previous runs of this step will be overwritten.
        :raise PreservationException:  if an error occurred preventing the application of this step.
        """
        raise NotImplementedError()

    @abstractmethod
    def revert(self, statemgr: PreservationStateManager) -> bool:
        """
        If possible, undo this preservation step.  If it cannot be (fully) undone by design, this 
        function will return without raising an exception.
        :return:  False if this step cannot be undone even partially
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        raise NotImplementedError()

    @abstractmethod
    def clean_up(self, statemgr: PreservationStateManager):
        """
        Clean up any unneeded state that was created while executing this step.  
        :raise PreservationException:  if an error occurred preventing the application of this step.
        """
        raise NotImplementedError()

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during preservation step clean-up (%s): %s",
                                 type(ex).__name__, str(ex))

    
class AIPFinalization(PreservationStep):
    """
    The interface for applying pre-serialization finalization to an AIP.  Finalization may include
    last-minute tweaks to the content of the AIP, but it also may involve extracting key information
    that will be needed in the :py:class:`AIPPublication` step. 

    This implementation can be instantiated and used to apply the null operation for finalization, 
    but it should also be used as a base class for specific finalization implementations.
    """

    def __init__(self):
        pass

    def apply(self, stagemgr: PreservationStateManager):
        """
        Apply the finalization steps.  This implementation does nothing.
        :raise AIPFinalizationException:  if an error occurred preventing the finalization
        """
        pass

    def revert(self, statemgr: PreservationStateManager) -> bool:
        """
        If possible, undo the finalization step.  Since :py:meth:`apply` does nothing, this 
        implementation does nothing but return True.
        :return:  False if this step cannot be undone even partially.
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        return True

    def clean_up(self, statemgr: PreservationStateManager):
        """
        Clean up any unneeded state that was created while executing this step.  Since :py:meth:`apply` 
        does nothing, this implementation does nothing.  
        :raise PreservationException:  if an error occurred while trying to clean-up this step
        """
        pass

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during finalization clean-up (%s): %s",
                                 type(ex).__name__, str(ex))

class AIPValidation(PreservationStep):
    """
    An abstract interface for validating an AIP's readiness for serialization and subsequent 
    archiving.  Implementations encapsulate a set of requirements that the underlying AIP 
    must meet.  

    Note that the :py:meth:`apply` method should raise an :py:class:`AIPNotValid` exception if 
    the AIP does not meet its validation requirements and :py:class:`AIPValidationException` if 
    a failure occurs while trying to apply the process itself.  
    """
    
    def revert(self, statemgr: PreservationStateManager) -> bool:
        """
        If possible, undo the finalization step.  As validation is 
        typically read-only, this default implementation does nothing but return True.
        :return:  False if this step cannot be undone even partially.
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        return True

    def clean_up(self, statemgr: PreservationStateManager):
        """
        Clean up any unneeded state that was created while executing this step.  As validation is 
        typically read-only, this default implementation does nothing.  
        :raise PreservationException:  if an error occurred while trying to clean-up this step
        """
        pass

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during validation clean-up (%s): %s",
                                 type(ex).__name__, str(ex))

class AIPSerialization(PreservationStep):
    """
    an abstract interface for serializing an AIP into one or more archivable files.  Subclasses
    implement a particular strategy for the serialization.
    """

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during serialization clean-up (%s): %s",
                                 type(ex).__name__, str(ex))

class AIPArchiving(PreservationStep):
    """
    an abstract interface for migrating serialized AIP files to long-term storage.  
    """

    @abstractmethod
    def submitted_to_archive(self, statemgr: PreservationStateManager) -> bool:
        """
        Submit the serialized AIP files for migration to long-term storage.  Migration could
        take up to days long to complete, so this function just starts the process.  Calling
        :py:meth:`transfer_complete` can be used to determine if the migration has finished.
        :raise AIPArchivingException:  if an error occurred preventing successful submission
                                       of the AIP files for transfer.
        """
        raise NotImplementedError()

    @abstractmethod
    def transfer_complete() -> bool:
        """
        Return True if all serialized AIP files have been fully migrated to long-term storage.  
        This is used to determine if the last step of the preservation process should be 
        completed.
        :raise PreservationException:  if an error occurred while trying to determine the state
                                       of the transfer
        """
        raise NotImplementedError()

    def apply(self, statemgr: PreservationStateManager):
        """
        Submit the serialized AIP files for migration to long-term storage.  By default,
        this trivially calls :py:meth:`submitted_to_archive`.  One should expect that the 
        outcome from any previous runs of this step will be overwritten.  
        :raise AIPArchivingException:  if an error occurred preventing successful submission
                                       of the AIP files for transfer.
        """
        self.submit_to_archive();

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during archiving clean-up (%s): %s",
                                 type(ex).__name__, str(ex))


class AIPPublication(PreservationStep):
    """
    an abstract interface for releasing an AIP that has been archived to external systems, namely a
    repository system through which the AIP can be discovered and accessed.  
    """
    
    def revert(self, statemgr: PreservationStateManager) -> bool:
        """
        If possible, undo the finalization step.  This implementation does nothing but return True.
        :return:  False if this step cannot be undone even partially.
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        return True

    def clean_up(self, statemgr: PreservationStateManager):
        """
        Clean up any unneeded state that was created while executing this step.  
        This implementation does nothing.  
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        pass

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during publication clean-up (%s): %s",
                                 type(ex).__name__, str(ex))


class PreservationTask(PreservationStepsAware):
    """
    a class representing the task of preserving a specific AIP that encapsulates the steps in the 
    process.  Each step is pluggable via the constructor.  This task keeps track of which steps have 
    been completed.

    This class is designed to support different strategies for preserving different kinds of AIPs, 
    separating that from how a preservation process is executed and managed.  The strategy for a
    particular preservation task is provided via the :py:class:`PreservationStep` instances 
    injected via the constructor.  A :py:class`PreservationStateManager` instance is used to 
    coordinate the execution of those steps.  A ``PreservationTask`` is normally created via a 
    :py:class:`PreservationTaskFactory`.  The design also allows for PreservationTasks to be resumed 
    after failures (and the source of the failure has been fixed). 
    """
    def __init__(self, mgr: PreservationStateManager, finalizer: AIPFinalization,
                 validater: AIPValidation, serializer: AIPSerialization, archiver: AIPArchiving,
                 publisher: AIPPublication):
        """
        initialize the task with its pluggable components.  Clients normally do not instantiate a 
        task directly, but rather call :py:meth:`PreservationTaskFactory.create_task` on a 
        :py:class:`PreservationTaskFactory`.  
        """
        self._statemgr = mgr
        self._finalizer = finalizer
        self._validater = validater
        self._serializer = serializer
        self._archiver = archiver
        self._publisher = publisher
        
    def _setup(self):
        # initialize the state manager
        self._statemgr.ensure_set_up()

    @property
    def aip_id(self):
        self._statemgr.aip_id

    def finalize(self) -> bool:
        """
        If it has not already been done, finalize the AIP in preparation for full preservation.
        This represents the first step in the preservation process.  If this step was already 
        completed, the function returns immediately.

        :return:  True if it was necessary to execute the step because it had not be carried out 
                  previously, or False because it was already completed.  
                  :rtype: bool
        :raises AIPFinalizationException:  if the process fails to complete this finalization step
                  successfully.  
        """
        if self.finalized():
            return False
        self._setup()
        self._finalizer.run(self._statemgr)
        return True

    def finalized(self) -> bool:
        """
        return True if the AIP has been finalized and is ready to be serialized.
        """
        return self._statemgr.state == self.FINALIZED

    def validate(self, as_is: bool=True) -> bool:
        """
        Validate that the AIP is ready for preservation.  This is the second step in the preservation
        process, coming after finalization.  
        :param bool as_is:  if False, ensure that the AIP has been finalized first; otherwise,
                  the validater will be forced to run on the AIP without regard to its state. 
        :return:  True if it was necessary to execute the step because it had not be carried out 
                  previously or becaues it was forced to by request via ``as_is``; otherwise, False 
                  is returned.
                  :rtype: bool
        :raises AIPValidationException:  if the process fails to complete this validation step
                  successfully.  
        :raises AIPNotValid:  if the AIP was found to be invalid or otherwise did not meet the 
                              requirements for preservation
        """
        if not as_is:
            if self.validated():
                return False
            self.finalize(self._statemgr)
        self._validater.run()
        return True

    def validated(self) -> bool:
        """
        return True if the AIP is considered currently valid according to the configured preservation
        requirements.  Note that this task may configured to always run the validater regardless before
        serialization; if this is the case, this will always return False.
        """
        self._statemgr.state == self.VALIDATED
            
    def serialize(self) -> bool:
        """
        Complete all preservation steps up through serialization.  If the AIP has already been 
        serialized completely, this function returns immediately.  
        :return:  True if it was necessary to execute the step because it had not be carried out 
                  previously, or False because it was already completed.  
                  :rtype: bool
        :raise AIPSerializationException:  if an error occurs during serialization
        """
        if self.serialized():
            return False
        self.validate(as_is=False)
        self._serializer.run(self._statemgr)
        return True

    def serialized(self) -> bool:
        """
        return True if the AIP has been completed serialized and is ready to be archived.
        """
        return self._statemgr.state == self.SERIALIZED

    def archive(self) -> bool:
        """
        Complete all preservation steps up through archiving.  Archiving--the process of committing 
        the serialized AIP to long-term storage--is itself inherently asynchronous, so this function 
        will start that process.  If the AIP has already been submitted for archiving, this function 
        returns immediately.  
        :return:  True if it was necessary to execute the step because it had not be carried out 
                  previously, or False because it was already completed.  
                  :rtype: bool
        :raise AIPArchivingException:  if an error occurred will initiating the archiving
        """
        if self.submitted_to_archive():
            return False
        self.serialize()
        self._archiver.submit(self._statemgr)
        return True

    def submitted_to_archive(self):
        """
        return True if this AIP has been serialized and submitted to long-term storage.  (The 
        transfer may still be underway.)
        """
        return self._statemgr.state == self.SUBMITTED

    def archived(self):
        """
        return True if this AIP has been completed migrated to long-term storage.  
        """
        return self._archiver.transfer_complete()

    def publish(self, ensure_archived=True):
        """
        Complete all preservation steps up through publishing.  If the AIP has already been published,
        this function returns immediately.  
        :param bool ensure_archived:  if True (default), make sure that the archiving process has 
                  been completed before publishing; if it is not, an Excpetion raised  If False, 
                  attempt to release the AIP while archiving is underway.
        :return:  True if it was necessary to execute the step because it had not be carried out 
                  previously, or False because it was already completed.  
                  :rtype: bool
        :raise AIPPublishingException:  if a fatal failure occurs while attempting to publish the AIP,
                  or if ``ensure_archived`` was True and archiving is not yet complete.
        """
        if self.published():
            return False
        self.archive()
        if ensure_archived and not self.archived():
            raise AIPPublishingException(id, "Archiving has not completed")
        self._publisher.run(self._statemgr)
        return True

    def published(self) -> bool:
        """
        return True if the AIP has completed the publication step (and, thus, the entire process).
        """
        return self._statemgr.state == self.PUBLISHED

class PreservationTaskFactory(metaclass=ABCMeta):
    """
    an interface for creating a :py:class:`PreservationTask` injected with necessary 
    :py:class:PreservationStep instances as specified by a given configuration.

    The factory methods require a configuration dictionary that complies with an expected 
    base structure that includes the following properties:

    :state_manager:  properties that configure the :py:class:`PreservationStateManager` to use
                     within the task.  (Some of the properties may be ignored by 
                     :py:meth:`recreate_task`.)
    :finalize:       properties that configure the :py:class:`AIPFinalizaiton` instance to use
                     (may be ignored by :py:meth:`recreate_task`).
    :validate:       properties that configure the :py:class:`AIPValidation` instance to use
                     (may be ignored by :py:meth:`recreate_task`).
    :serialize:      properties that configure the :py:class:`AIPSerialization` instance to use
                     (may be ignored by :py:meth:`recreate_task`).
    :archive:        properties that configure the :py:class:`AIPArchiving` instance to use
                     (may be ignored by :py:meth:`recreate_task`).
    :publish:        properties that configure the :py:class:`AIPPublication` instance to use
                     (may be ignored by :py:meth:`recreate_task`).

    Each of the above properties have dictionary values which can include the ``type`` property.
    This property identifies the class that should be instantiated to handle its part of the 
    preservation process.  The value expected--in particular, whether this it is a label that 
    maps to a class, is an explicit python class name, etc.--is implementation dependent.  (A
    factory implementation may limit which classes are supported.)  If the ``type`` property is not 
    provided, the factory may assume a default.  All other subproperties expected depends on the 
    component class implementation.

    Note that the factory takes responsibility for sharing subproperties across the top-level 
    dictionaries to ensure that the steps work together--i.e. that a step can find the outputs of 
    the previous step.  
    """

    def create_task(self, aipid: str, config: Mapping, logger: Logger=None) -> PreservationTask:
        """
        create the fully configured :py:class:`PreservationTask` that preserve a given AIP.  
        :param str      aipid:  the identifier of the AIP to preserve.  The ``config`` must 
                                indicate where this AIP is located.
        :param Mapping config:  the configuration that controls construction of this task and
                                the behavior of the steps.
        :param Logger  logger:  the logger to use during preservation
        :return:  the configured :py:class:`PreservationTask`
        :raise ConfigurationException:  if the task cannot be created due to insufficient or 
                                incorrect configuration
        :raise PreservationException:  if any other failure occurs while assembling the task.
        """
        pass

    def recreate_task(self, aipid: str, config: Mapping, logger: Logger=None) -> PreservationTask:
        """
        recreate the task from its existing persisted state so that it can be resumed, canceled, or
        cleaned-up (e.g. after a system failure).  This function will only look at the configuration
        will only look at the ``state_manager`` property to determine where/how the current can 
        be found; all other properties will be overridden by the configuration persisted when the 
        task was originally created.

        :param str      aipid:  the identifier of the AIP to preserve.  The ``config`` must 
                                indicate where this AIP is located.
        :param Mapping config:  the configuration that controls construction of this task and
                                the behavior of the steps.
        :param Logger  logger:  the logger to use during preservation
        :return:  the configured :py:class:`PreservationTask`
        :raise ConfigurationException:  if the task cannot be created due to insufficient or 
                                incorrect configuration
        :raise PreservationException:  if any other failure occurs while assembling the task.
        """
        pass

    @abstractmethod
    def _create_state_manager(self, aipid: str, config: Mapping, logger: Logger) -> PreservationStateManager:
        raise NotImplementedError()

    @abstractmethod
    def _create_finalizer(self, config: Mapping) -> AIPFinalization:
        raise NotImplementedError()

    @abstractmethod
    def _create_validater(self, config: Mapping) -> AIPValidation:
        raise NotImplementedError()

    @abstractmethod
    def _create_serializer(self, config: Mapping) -> AIPSerialization:
        raise NotImplementedError()

    @abstractmethod
    def _create_archiver(self, config: Mapping) -> AIPArchiving:
        raise NotImplementedError()

    @abstractmethod
    def _create_publisher(self, config: Mapping) -> AIPPublication:
        raise NotImplementedError()
        

