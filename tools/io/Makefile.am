#
# This file is part of bgpstream
#
# Copyright (C) 2015 The Regents of the University of California.
# Authors: Alistair King, Chiara Orsini
#
# All rights reserved.
#
# This code has been developed by CAIDA at UC San Diego.
# For more information, contact bgpstream-info@caida.org
#
# This source code is proprietary to the CAIDA group at UC San Diego and may
# not be redistributed, published or disclosed without prior permission from
# CAIDA.
#
# Report any bugs, questions or comments to bgpstream-info@caida.org
#

AM_CPPFLAGS = -I$(top_srcdir) \
	      -I$(top_srcdir)/common \
	      -I$(top_srcdir)/lib \
	      -I$(top_srcdir)/lib/utils \
	      -I$(top_srcdir)/bgpview/lib \
	      -I$(top_srcdir)/bgpview/lib/io \
	      -I$(top_srcdir)/bgpview/lib/consumers

TOOL_LIBS = -L$(top_builddir)/lib -L$(top_builddir)/bgpview/lib

dist_bin_SCRIPTS =

bin_PROGRAMS =	bgpwatcher-server \
		bgpwatcher-consumer \
		bgpwatcher-test-producer

# runs the bgpwatcher server
bgpwatcher_server_SOURCES = \
	bgpwatcher-server.c
bgpwatcher_server_LDADD = -lbgpwatcher -lbgpstream
bgpwatcher_server_LDFLAGS = $(TOOL_LIBS)


# connects to the bgpwatcher server and drives consumer plugins
bgpwatcher_consumer_SOURCES = \
	bgpwatcher-consumer.c
bgpwatcher_consumer_LDADD = -lbgpwatcher -lbgpstream
bgpwatcher_consumer_LDFLAGS = $(TOOL_LIBS)

# acts as a dummy producer for the bgpwatcher server
bgpwatcher_test_producer_SOURCES = \
	bgpwatcher-test-producer.c
bgpwatcher_test_producer_LDADD = -lbgpwatcher -lbgpstream
bgpwatcher_test_producer_LDFLAGS = $(TOOL_LIBS)

ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
