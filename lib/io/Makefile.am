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

noinst_LTLIBRARIES = libbgpview_io.la

include_HEADERS =

libbgpview_io_la_SOURCES = 		\
	bgpview_io_common.h		\
	bgpview_io_common_int.h		\
	bgpview_io_common.c		\
	bgpview_io_client.h		\
	bgpview_io_client_int.h		\
	bgpview_io_client.c		\
	bgpview_io_client_broker.h	\
	bgpview_io_client_broker.c	\
	bgpview_io_server.h		\
	bgpview_io_server_int.h		\
	bgpview_io_server.c		\
	bgpview_io_store.h		\
	bgpview_io_store.c		\
	bgpview_io.h		\
	bgpview_io.c

libbgpview_io_la_LIBADD =

ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
