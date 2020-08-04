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

SUBDIRS = plugins
AM_CPPFLAGS = -I$(top_srcdir) \
	      -I$(top_srcdir)/lib \
	      -Iplugins \
	      -I$(top_srcdir)/common

lib_LTLIBRARIES = libbgpcorsarocaida.la

include_HEADERS = bgpcorsaro.h

libbgpcorsarocaida_la_SOURCES = 	\
	bgpcorsaro.c 		\
	bgpcorsaro.h 		\
	bgpcorsaro_int.h 	\
	bgpcorsaro_io.c 	\
	bgpcorsaro_io.h 	\
	bgpcorsaro_log.c	\
	bgpcorsaro_log.h	\
	bgpcorsaro_plugin.h

libbgpcorsarocaida_la_LIBADD = $(top_builddir)/common/libcccommon.la \
		          plugins/libbgpcorsaroplugins.la

ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
