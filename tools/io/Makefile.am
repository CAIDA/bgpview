#
# This file is part of bgpstream
#
# CAIDA, UC San Diego
# bgpstream-info@caida.org
#
# Copyright (C) 2012 The Regents of the University of California.
# Authors: Alistair King, Chiara Orsini
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see <http://www.gnu.org/licenses/>.
#

AM_CPPFLAGS = -I$(top_srcdir) \
	      -I$(top_srcdir)/common \
	      -I$(top_srcdir)/lib \
	      -I$(top_srcdir)/lib/utils \
	      -I$(top_srcdir)/bgpview/lib \
	      -I$(top_srcdir)/bgpview/lib/io \
	      -I$(top_srcdir)/bgpview/lib/consumers

dist_bin_SCRIPTS =

bin_PROGRAMS =	bgpview-server		\
		bgpview-consumer	\
		bgpview-producer	\
		bgpview-test-producer	\
		bvcat

# runs the bgpview server
bgpview_server_SOURCES = \
	bgpview-server.c
bgpview_server_LDADD = $(top_builddir)/lib/libbgpstreamcaida.la \
		       $(top_builddir)/bgpview/lib/libbgpviewcaida.la

# connects to the bgpview server and drives consumer plugins
bgpview_consumer_SOURCES = \
	bgpview-consumer.c
bgpview_consumer_LDADD = $(top_builddir)/lib/libbgpstreamcaida.la \
		         $(top_builddir)/bgpview/lib/libbgpviewcaida.la

# deserializes BGPViews and sends them to the server
bgpview_producer_SOURCES = \
	bgpview-producer.c
bgpview_producer_LDADD = $(top_builddir)/lib/libbgpstreamcaida.la \
		         $(top_builddir)/bgpview/lib/libbgpviewcaida.la

# acts as a dummy producer for the bgpview server
bgpview_test_producer_SOURCES = \
	bgpview-test-producer.c
bgpview_test_producer_LDADD = $(top_builddir)/lib/libbgpstreamcaida.la \
			      $(top_builddir)/bgpview/lib/libbgpviewcaida.la

# BGPView Cat
bvcat_SOURCES = \
	bvcat.c
bvcat_LDADD = $(top_builddir)/bgpview/lib/libbgpviewcaida.la

ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
