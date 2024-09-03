.. -*- rst -*-

===========
Diagnostics
===========

Gets diagnostic info about the Coriolis appliance.


List Diagnostics
================

.. rest_method:: GET /diagnostics

Lists diagnostic data from all Coriolis services.

Normal response codes: 200

Error response codes:   unauthorized(401),
forbidden(403)

Response
--------

.. rest_parameters:: parameters.yaml

  - diagnostics : diagnostics_array
  - application : diagnostic_application
  - os_info : diagnostic_os_info
  - hostname : diagnostic_hostname
  - ip_addresses : diagnostic_ip_addresses
  - licensing_status : diagnostic_licensing_status
  - packages : diagnostic_packages
  - licences : diagnostic_licences
  - reservations : diagnostic_reservations

**Example of a Diagnostics List**

  .. literalinclude:: ../api_samples/diagnostics/diagnostics-get-resp.json
    :language: javascript

