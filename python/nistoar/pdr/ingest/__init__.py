"""
modules for ingesting Archive Information Package (AIP) artifacts into the public PDR and partner systems.

These modules include:
  * ``lts`` -- a module for delivering serialized AIPs to long-term storage
  * ``pdr`` -- a module for loading metadata and support files into the public PDR system
  * ``edi`` -- a module for delivering POD records derived from AIP NERDm data to the NIST EDI
  * ``dc``  -- a module for delivering DataCite records to DataCite

Each module contains a submodule called ``client`` which provides a client class for use by the 
PDR publishing system to submit artifacts.  Additional modules may be included to provice service 
implementations to be run on the public repository side to ingest the artifacts.  
"""
