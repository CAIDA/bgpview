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

bin_PROGRAMS =	bgpview-server \
		bgpview-consumer \
		bgpview-test-producer

# runs the bgpview server
bgpview_server_SOURCES = \
	bgpview-server.c
bgpview_server_LDADD = -lbgpview -lbgpstream
bgpview_server_LDFLAGS = $(TOOL_LIBS)


# connects to the bgpview server and drives consumer plugins
bgpview_consumer_SOURCES = \
	bgpview-consumer.c
bgpview_consumer_LDADD = -lbgpview -lbgpstream
bgpview_consumer_LDFLAGS = $(TOOL_LIBS)

# acts as a dummy producer for the bgpview server
bgpview_test_producer_SOURCES = \
	bgpview-test-producer.c
bgpview_test_producer_LDADD = -lbgpview -lbgpstream
bgpview_test_producer_LDFLAGS = $(TOOL_LIBS)

ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
