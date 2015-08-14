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

AM_CPPFLAGS = 	-I$(top_srcdir)			\
		-I$(top_srcdir)/common 		\
		-I$(top_srcdir)/lib		\
		-I$(top_srcdir)/lib/utils	\
		-I$(top_srcdir)/bgpview/lib	\
		-I$(top_srcdir)/bgpview/lib/consumers

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
