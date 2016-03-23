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

SUBDIRS =

AM_CPPFLAGS = 	-I$(top_srcdir) \
	 	-I$(top_srcdir)/common \
                -I$(top_srcdir)/lib \
                -I$(top_srcdir)/lib/utils \
                -I$(top_srcdir)/bgpview/lib \
                -I$(top_srcdir)/bgpview/lib/consumers

noinst_LTLIBRARIES = libbgpview_io.la

include_HEADERS =

libbgpview_io_la_SOURCES = 		\
	bgpview_io.c			\
	bgpview_io.h

MOD_LIBS=

if WITH_BGPVIEW_IO_FILE
SUBDIRS+=file
MOD_LIBS+=$(top_builddir)/bgpview/lib/io/file/libbgpview_io_file.la
endif

if WITH_BGPVIEW_IO_ZMQ
SUBDIRS+=zmq
MOD_LIBS+=$(top_builddir)/bgpview/lib/io/zmq/libbgpview_io_zmq.la
endif

if WITH_BGPVIEW_IO_KAFKA
SUBDIRS+=kafka
MOD_LIBS+=$(top_builddir)/bgpview/lib/io/kafka/libbgpview_io_kafka.la
endif

libbgpview_io_la_LIBADD = \
	$(MOD_LIBS)


ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
