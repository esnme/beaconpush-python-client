Beaconpush Python Client
========================

A Python client for [Beaconpush][Beaconpush], a real-time messaging server.
This module ships with two clients, one for sending messages to the server and one for event listening.
The client follows the same partitioning scheme as the Beaconpush server and has built-in support for connecting to a cluster of servers.
It is only compatible with the On-Site edition of Beaconpush.

Installing
----------
The package is available from [PyPI][PyPI], (the Python Package Index aka "The Cheese Shop").
To install the client use either pip (recommended) or easy_install:

    pip install beaconpush-client


Releasing a new version
-----------------------

Releasing a new version requires a bit of manual work. Perhaps this can be (more) automated in the future.
But for now, the following steps do the job:

1. Commit your changes and make sure you don't have uncommitted files inside the modules.
1. Update the version number inside the module (follow semantic versioning).
1. Make sure the new version number is reflected by running ```python setup.py --version```.
1. Commit your change of version.
1. Tag your new commit as a stable version. You can do this by running ```git tag $(python setup.py --version)```. Then make sure you push this new tag ```git push origin $(python setup.py --version)```.
1. Run ```python setup.py clean sdist``` to create a source distribution.
1. Upload this to your Python repository (we run an internal PyPI repo at ESN requiring you to upload the sdist yourself)
1. Set the new development version to be used and commit the change.


Author
-------

  - <a href="http://www.esn.me">ESN Social Software</a> ([@esnme](http://twitter.com/esnme))


License
-------

Open-source licensed under the MIT license (see _LICENSE_ file for details).


[Beaconpush]: http://beaconpush.com
[PyPI]: http://pypi.python.org
[RRD]: http://oss.oetiker.ch/rrdtool/
[Graphite]: http://graphite.wikidot.com
[Whisper]: http://graphite.wikidot.com/whisper
