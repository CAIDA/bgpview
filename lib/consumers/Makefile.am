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

AM_CPPFLAGS = 	-I$(top_srcdir)			\
		-I$(top_srcdir)/common 		\
		-I$(top_srcdir)/lib		\
		-I$(top_srcdir)/lib/utils	\
		-I$(top_srcdir)/bgpview/lib	\
		-I$(top_srcdir)/bgpview/lib/io	\
		-I$(top_srcdir)/bgpview/lib/consumers

# BGPView IO is needed for bgpview_io_dump and bgpview_io_write

noinst_LTLIBRARIES = libbgpview_consumers.la

include_HEADERS =

CONSUMER_SRCS=
CONSUMER_LIBS=

# Test Consumer
CONSUMER_SRCS += \
	bvc_test.c \
	bvc_test.h

# Performance Monitor Consumer
CONSUMER_SRCS += \
	bvc_perfmonitor.c \
	bvc_perfmonitor.h

# Visibility Consumer
CONSUMER_SRCS += \
	bvc_visibility.c \
	bvc_visibility.h

# Per-AS Visibility Consumer (depends on Visibility)
CONSUMER_SRCS += \
	bvc_perasvisibility.c \
	bvc_perasvisibility.h

# Per-Geo Visibility Consumer (depends on Visibility)
CONSUMER_SRCS += \
	bvc_pergeovisibility.c \
	bvc_pergeovisibility.h

# Announced Prefixes Consumer (depends on Visibility)
CONSUMER_SRCS += \
	bvc_announcedpfxs.c \
	bvc_announcedpfxs.h

# Moas Consumer (depends on Visibility)
CONSUMER_SRCS += \
	bvc_moas.c \
	bvc_moas.h


# -- sample how to add conditional consumer
#if WITH_<NAME>
#SUBDIRS += lib<name>
#AM_CPPFLAGS += -I$(top_srcdir)/lib/consumers/lib<name>
#CONSUMER_SRCS += \
#	bvc_<name>.c \
#	bvc_<name>.h
#CONSUMER_LIBS += $(top_builddir)/lib/consumers/lib<name>/lib<name>.la
#endif

libbgpview_consumers_la_SOURCES = bgpview_consumer_manager.h   \
				  bgpview_consumer_manager.c   \
				  bgpview_consumer_interface.h \
				  $(CONSUMER_SRCS)

libbgpview_consumers_la_LIBADD = $(CONSUMER_LIBS)

ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
