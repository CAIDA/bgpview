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

SUBDIRS = consumers io

AM_CPPFLAGS = 	-I$(top_srcdir) \
	 	-I$(top_srcdir)/common \
                -I$(top_srcdir)/lib \
                -I$(top_srcdir)/lib/utils \
                -I$(top_srcdir)/bgpview/lib/consumers \
                -I$(top_srcdir)/bgpview/lib/io

lib_LTLIBRARIES = libbgpwatcher.la

include_HEADERS = 			\
	bgpwatcher_view.h

libbgpwatcher_la_SOURCES = 		\
	bgpwatcher_view.h		\
	bgpwatcher_view.c

libbgpwatcher_la_LIBADD = $(top_builddir)/lib/libbgpstream.la \
			  $(top_builddir)/common/libcccommon.la \
			  $(top_builddir)/bgpview/lib/io/libbgpwatcher_io.la \
			  $(top_builddir)/bgpview/lib/consumers/libbgpwatcher_consumers.la

libbgpwatcher_la_LDFLAGS = -version-info @BGPSTREAM_MAJOR_VERSION@:@BGPSTREAM_MID_VERSION@:@BGPSTREAM_MINOR_VERSION@

ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
