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

AM_CPPFLAGS = 	-I$(top_srcdir) \
	 	-I$(top_srcdir)/common \
                -I$(top_srcdir)/lib

noinst_LTLIBRARIES = libbgpview_io.la

include_HEADERS = bgpview_io.h

libbgpview_io_la_SOURCES = 		\
	bgpview_io.c			\
	bgpview_io.h

MOD_LIBS=

if WITH_BGPVIEW_IO_FILE
SUBDIRS+=file
MOD_LIBS+=$(top_builddir)/lib/io/file/libbgpview_io_file.la
endif

if WITH_BGPVIEW_IO_ZMQ
SUBDIRS+=zmq
MOD_LIBS+=$(top_builddir)/lib/io/zmq/libbgpview_io_zmq.la
endif

if WITH_BGPVIEW_IO_KAFKA
SUBDIRS+=kafka
MOD_LIBS+=$(top_builddir)/lib/io/kafka/libbgpview_io_kafka.la
endif

if WITH_BGPVIEW_IO_BSRT
SUBDIRS+=bsrt
MOD_LIBS+=$(top_builddir)/lib/io/bsrt/libbgpview_io_bsrt.la
endif

if WITH_BGPVIEW_IO_TEST
SUBDIRS+=test
MOD_LIBS+=$(top_builddir)/lib/io/test/libbgpview_io_test.la
endif

libbgpview_io_la_LIBADD = \
	$(MOD_LIBS)


ACLOCAL_AMFLAGS = -I m4

CLEANFILES = *~
