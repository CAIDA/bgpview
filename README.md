BGPCorsaro
=========

BGPCorsaro is part of the [BGPStream framework](https://bgpstream.caida.org).

BGPCorsaro is a plugin-based library and tool for the analysis of real-time
Border Gateway Protocol (BGP) measurement data.

For a detailed description of BGPCorsaro as well as documentation and tutorials,
please visit http://bgpstream.caida.org.

Quick Start
-----------

To get started using BGPCorsaro, either download the latest
[release tarball](http://bgpstream.caida.org/download), or clone the
[GitHub repository](https://github.com/CAIDA/bgpcorsaro).

You will need [libbgpstream](http://bgpstream.caida.org/download) installed
prior to building BGPCorsaro.

In most cases, the following will be enough to build and install BGPCorsaro:
~~~
$ ./configure
$ make
# make install
~~~

If you cloned BGPCorsaro from GitHub, you will need to run `./autogen.sh` before
`./configure`.

For further information or support, please visit the
[BGPStream website](http://bgpstream.caida.org), or contact
bgpstream-info@caida.org.
