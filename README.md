sehrvy
======

Motivation
----------

_sehrvy_ is an example project to explore server creation and database usage with Perl, and graph vizualization with JavaScript.

The nominal goal is to create a server that visualizes [Meaningful Use reimbursements for multiple healhcare providers][1]. Once we figure out how to do that, we'll have it serve pretty graphics upon request. The details still remain to be worked out.

  [1]: http://explore.data.gov/Science-and-Technology/CMS-Medicare-and-Medicaid-EHR-Incentive-Program-el/8pfj-qf8a "Meaningful Use Data"

The real goal is just to try stuff and learn stuff.

Requires
--------

 * `DBD::mysql`
 * `Net::Sever::HTTP`


Try it out
----------

You can try running the server (defaults to port 8080), by executing the `run_server` script in `src/`.
