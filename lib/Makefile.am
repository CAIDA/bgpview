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
