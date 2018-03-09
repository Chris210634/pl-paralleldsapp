##############
pl-simpledsapp
##############


Abstract
********

This simple plugin demonstrates how to run the "Data System" (DS) class of plugin in ChRIS. This DS type is used to create additional data trees rooted in a particular level of a Feed hierarchy.

Preconditions
*************

This plugin requires an input directory, typically created by ``simplefsapp.py`` as precondition.

Run
***

Using ``docker run``
====================

Assign an "input" directory to ``/incoming`` and an output directory to ``/outgoing``

.. code-block:: bash

    docker run -v $(pwd)/out:/incoming -v $(pwd)/out2:/outgoing     \
            fnndsc/pl-simpledsapp simpledsapp.py                    \
            --prefix test-                                          \
            --sleepLength 0                                         \
            /incoming /outgoing

The above will create a copy of each file in the container's ``/incoming`` and prefix the copy with the ``prefix`` text (in this case "test-"). The copies will be storeed in the container's ``/outgoing`` directory.

Make sure that the host ``$(pwd)/out2`` directory is world writable!

Example
=======

.. code-block:: bash

    docker run -v $(pwd)/out:/incoming -v $(pwd)/out2:/outgoing     \
            fnndsc/pl-simpledsapp simpledsapp.py                    \
            --prefix test-                                          \
            --sleepLength 0                                         \
            /incoming /outgoing

.. code-block:: bash

  $>ls -la out2
  total 20K
  drwxrwxrwx 2 rudolph fnndsc   12K May  8 16:41 ./
  drwxrwxr-x 4 rudolph fnndsc  4.0K May  8 16:40 ../
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-aa-remove-unknown
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-aa-status
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-accept
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-accessdb
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-acpid
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-addgnupghome
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-addgroup
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-add-shell
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-adduser
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-alsabat-test
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-alsactl
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-alsa-info
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-anacron
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-apparmor_status
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-applygnupgdefaults
  -rw-r--r-- 1 nobody  nogroup    0 May  8 16:41 test-aptd
  ...






