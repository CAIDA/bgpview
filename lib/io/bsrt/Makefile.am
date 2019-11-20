#
# This file is part of bgpstream
#
# CAIDA, UC San Diego
# bgpstream-info@caida.org
#
# Copyright (C) 2019 The Regents of the University of California.
# Authors: Ken Keys
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

SUBDIRS = libroutingtables libbgpcorsaro

AM_CPPFLAGS = 	-I$(top_srcdir) \
	 	-I$(top_srcdir)/common \
                -I$(top_srcdir)/lib \
                -I$(top_srcdir)/lib/io

noinst_LTLIBRARIES = libbgpview_io_bsrt.la

include_HEADERS =

libbgpview_io_bsrt_la_SOURCES = 	\
	bgpview_io_bsrt.c		\
	bgpview_io_bsrt.h

libbgpview_io_bsrt_la_LIBADD =
libbgpview_io_bsrt_la_LIBADD += $(top_srcdir)/lib/io/bsrt/libroutingtables/libroutingtables.la
libbgpview_io_bsrt_la_LIBADD += $(top_srcdir)/lib/io/bsrt/libbgpcorsaro/libbgpcorsarocaida.la
AM_CPPFLAGS += -I$(top_srcdir)/lib/io/bsrt/libroutingtables
AM_CPPFLAGS += -I$(top_srcdir)/lib/io/bsrt/libbgpcorsaro


ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
