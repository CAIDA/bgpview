#
# Copyright (C) 2014 The Regents of the University of California.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

SUBDIRS =

AM_CPPFLAGS = 	-I$(top_srcdir)				\
		-I$(top_srcdir)/common 			\
		-I$(top_srcdir)/lib			\
		-I$(top_srcdir)/lib/io			\
		-I$(top_srcdir)/lib/consumers

# BGPView IO is needed for bgpview_io_dump and bgpview_io_write

noinst_LTLIBRARIES = libbgpview_consumers.la

include_HEADERS = bgpview_consumer_manager.h

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

if WITH_BGPVIEW_IO_FILE
# Archiver consumer
CONSUMER_SRCS += \
	bvc_archiver.c \
	bvc_archiver.h
endif

# Submoas consumer
CONSUMER_SRCS += \
	bvc_submoas.c \
	bvc_submoas.h

# Edges consumer
CONSUMER_SRCS += \
	bvc_edges.c \
	bvc_edges.h

# Triplets consumer
CONSUMER_SRCS += \
	bvc_triplets.c \
	bvc_triplets.h

# Pfxorigins consumer
CONSUMER_SRCS += \
	bvc_pfxorigins.c \
	bvc_pfxorigins.h

# Peerpfxorigins consumer
CONSUMER_SRCS += \
        bvc_peerpfxorigins.c \
        bvc_peerpfxorigins.h

# Routed Space Consumer (no dependencies)
CONSUMER_SRCS += \
	bvc_routedspace.c \
	bvc_routedspace.h

# My View Process Consumer (no dependencies)
CONSUMER_SRCS += \
	bvc_myviewprocess.c \
	bvc_myviewprocess.h

# Alistair can't figure out how to do an OR...
if WITH_BGPVIEW_IO_KAFKA
# view sender
CONSUMER_SRCS += \
	bvc_viewsender.c  \
	bvc_viewsender.h
else
if WITH_BGPVIEW_IO_ZMQ
# view sender
CONSUMER_SRCS += \
	bvc_viewsender.c  \
	bvc_viewsender.h
endif
endif

# Path Change (no dependencies)
CONSUMER_SRCS += \
	bvc_pathchange.c \
	bvc_pathchange.h

# Subpfx consumer
CONSUMER_SRCS += \
	bvc_subpfx.c \
	bvc_subpfx.h

# Prefix-to-AS consumer
CONSUMER_SRCS += \
	bvc_pfx2as.c \
	bvc_pfx2as.h

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
				  bgpview_consumer_utils.h   \
				  bgpview_consumer_utils.c   \
				  bgpview_consumer_interface.h \
				  $(CONSUMER_SRCS)

libbgpview_consumers_la_LIBADD = $(CONSUMER_LIBS)

ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
