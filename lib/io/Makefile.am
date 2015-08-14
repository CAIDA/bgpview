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

SUBDIRS =

AM_CPPFLAGS = 	-I$(top_srcdir) \
	 	-I$(top_srcdir)/common \
                -I$(top_srcdir)/lib \
                -I$(top_srcdir)/lib/utils \
                -I$(top_srcdir)/bgpview/lib

noinst_LTLIBRARIES = libbgpwatcher_io.la

include_HEADERS =

libbgpwatcher_io_la_SOURCES = 		\
	bgpwatcher_common.h		\
	bgpwatcher_common_int.h		\
	bgpwatcher_common.c		\
	bgpwatcher_client.h		\
	bgpwatcher_client_int.h		\
	bgpwatcher_client.c		\
	bgpwatcher_client_broker.h	\
	bgpwatcher_client_broker.c	\
	bgpwatcher_server.h		\
	bgpwatcher_server_int.h		\
	bgpwatcher_server.c		\
	bgpwatcher_store.h		\
	bgpwatcher_store.c		\
	bgpwatcher_view_io.h		\
	bgpwatcher_view_io.c

libbgpwatcher_io_la_LIBADD =

ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
