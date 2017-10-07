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

SUBDIRS = io consumers

AM_CPPFLAGS = 	-I$(top_srcdir) \
	 	-I$(top_srcdir)/common \
                -I$(top_srcdir)/lib/consumers \
                -I$(top_srcdir)/lib/io

lib_LTLIBRARIES = libbgpview.la

include_HEADERS = 			\
	bgpview.h

libbgpview_la_SOURCES = 	\
	bgpview.h		\
	bgpview.c		\
	bgpview_debug.c		\
	bgpview_debug.h

libbgpview_la_LIBADD = \
	$(top_builddir)/common/libcccommon.la \
	$(top_builddir)/lib/io/libbgpview_io.la \
	$(top_builddir)/lib/consumers/libbgpview_consumers.la

libbgpview_la_LDFLAGS = -version-info @LIBBGPVIEW_MAJOR_VERSION@:@LIBBGPVIEW_MID_VERSION@:@LIBBGPVIEW_MINOR_VERSION@

ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
