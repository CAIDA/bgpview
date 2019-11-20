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

AM_CPPFLAGS = -I$(top_srcdir) \
	      -I$(top_srcdir)/lib \
	      -I$(top_srcdir)/common

noinst_LTLIBRARIES = libbgpcorsaroplugins.la

include_HEADERS =
PLUGIN_SRC=
PLUGIN_LIBS=

PLUGIN_LIBS += $(top_builddir)/lib/io/bsrt/libroutingtables/libroutingtables.la
AM_CPPFLAGS += -I$(top_srcdir)/lib/io/bsrt/libbgpcorsaro
AM_CPPFLAGS += -I$(top_srcdir)/lib/io/bsrt/libroutingtables
PLUGIN_SRC+=bgpcorsaro_routingtables.c bgpcorsaro_routingtables.h

libbgpcorsaroplugins_la_SOURCES = \
	$(PLUGIN_SRC)

libbgpcorsaroplugins_la_LIBADD = \
	$(PLUGIN_LIBS)

ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
