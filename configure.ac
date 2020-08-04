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

AC_PREREQ([2.68])

AC_INIT([bgpview], [2.0.0], [bgpstream-info@caida.org])
AM_INIT_AUTOMAKE([foreign])

BGPVIEW_MAJOR_VERSION=2
BGPVIEW_MID_VERSION=0
BGPVIEW_MINOR_VERSION=0

LIBBGPVIEW_MAJOR_VERSION=2
LIBBGPVIEW_MID_VERSION=0
LIBBGPVIEW_MINOR_VERSION=0

AC_DEFINE_UNQUOTED([BGPVIEW_MAJOR_VERSION],$BGPVIEW_MAJOR_VERSION,
        [bgpview major version])
AC_DEFINE_UNQUOTED([BGPVIEW_MID_VERSION],$BGPVIEW_MID_VERSION,
        [bgpview mid version])
AC_DEFINE_UNQUOTED([BGPVIEW_MINOR_VERSION],$BGPVIEW_MINOR_VERSION,
        [bgpview minor version])

AC_SUBST([BGPVIEW_MAJOR_VERSION])
AC_SUBST([BGPVIEW_MID_VERSION])
AC_SUBST([BGPVIEW_MINOR_VERSION])

AC_DEFINE_UNQUOTED([LIBBGPVIEW_MAJOR_VERSION],$LIBBGPVIEW_MAJOR_VERSION,
        [libbgpview major version])
AC_DEFINE_UNQUOTED([LIBBGPVIEW_MID_VERSION],$LIBBGPVIEW_MID_VERSION,
        [libbgpview mid version])
AC_DEFINE_UNQUOTED([LIBBGPVIEW_MINOR_VERSION],$LIBBGPVIEW_MINOR_VERSION,
        [libbgpview minor version])

AC_SUBST([LIBBGPVIEW_MAJOR_VERSION])
AC_SUBST([LIBBGPVIEW_MID_VERSION])
AC_SUBST([LIBBGPVIEW_MINOR_VERSION])

LT_INIT

# Check if we should disable rpath.
#
# For advanced users: In certain configurations (e.g. when one of Bgpview's
# dependencies is located in a directory which also holds an old
# libbgpview.so), the rpath attributes added by libtool cause problems with
# make check (or indeed with any non-installed binaries) as rpath will be
# preferred over LD_LIBRARY_PATH. This does not seem to be a problem with
# clang. When using --disable-rpath you will likely need to set LD_LIBRARY_PATH
# if you are using libraries in non-system locations. YMMV.
#
DISABLE_RPATH

AM_INIT_AUTOMAKE

AC_CONFIG_MACRO_DIR([m4])

AC_CONFIG_SRCDIR([lib/bgpview.c])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_LIBTOOL
AC_PROG_CXX
AM_PROG_LIBTOOL
AC_PROG_CC_C99 # C99 is required because of bool

AC_SYS_LARGEFILE

AH_VERBATIM([_GNU_SOURCE],
[/* Enable GNU extensions on systems that have them.  */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif])

AC_CHECK_FUNCS([gettimeofday memset strdup strstr strsep strlcpy vasprintf])

# should we dump debug output to stderr and not optmize the build?

AC_MSG_CHECKING([whether to build with debug information])
AC_ARG_ENABLE([debug],
    [AS_HELP_STRING([--enable-debug],
        [enable debug data generation (def=no)])],
    [debugit="$enableval"],
    [debugit=no])
AC_MSG_RESULT([$debugit])

if test x"$debugit" = x"yes"; then
    AC_DEFINE([DEBUG],[],[Debug Mode])
fi

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC

# Checks for header files.
AC_CHECK_HEADERS([arpa/inet.h inttypes.h limits.h math.h stdlib.h string.h \
			      time.h sys/time.h])

# Checks for mandatory libraries

# this code is needed to get the right threading library on a mac
STASH_CFLAGS="$CFLAGS"
CFLAGS=
AX_PTHREAD(, [AC_MSG_ERROR([pthreads required])])
CFLAGS="$STASH_CFLAGS"
LIBS="$PTHREAD_LIBS $LIBS"
CFLAGS="$CFLAGS $PTHREAD_CFLAGS"
CC="$PTHREAD_CC"

# Check that BGPStream is installed
AC_CHECK_LIB([bgpstream], [bgpstream_create], ,
  [AC_MSG_ERROR( [libbgpstream is required (https://bgpstream.caida.org)])])


# CONSUMERS
# TODO: move consumers into separate repos (IODA and BGP-Hijacks)
# TODO: Make BGPView Consumers optional
AC_MSG_NOTICE([checking BGPView Consumer dependencies])

AC_CHECK_LIB([timeseries], [timeseries_init], , [
    AC_MSG_ERROR([libtimeseries is required for the Per-AS Visibility plugin])
])
AC_CHECK_LIB([ipmeta], [ipmeta_record_set_clear], ,[AC_MSG_ERROR([ipmeta is required])])


# BGPView IO modules
# TODO: Make BGPView-IO optional (one may just want the datastructure without IO/Consumers)
AC_MSG_NOTICE([configuring BGPView IO modules])

# Test module
BS_WITH_IO_MOD([test],[TEST],[yes])

# File module
BS_WITH_IO_MOD([file],[FILE],[yes])

# BGPStream RoutingTable module
BS_WITH_IO_MOD([bsrt],[BSRT],[yes])

# ZMQ module
BS_WITH_IO_MOD([zmq],[ZMQ],[no])

# Kafka module
BS_WITH_IO_MOD([kafka],[KAFKA],[yes])

# BGPView IO dependencies
AC_MSG_NOTICE([checking BGPView IO module dependencies])

if test "x$with_io_file" = xyes; then
   AC_SEARCH_LIBS([wandio_create], [wandio], [with_wandio=yes],
               [AC_MSG_ERROR(
     [libwandio required (http://research.wand.net.nz/software/libwandio.php)
     for the file IO module]
   )])
fi
AM_CONDITIONAL([WITH_WANDIO], [test "x$with_wandio" = xyes])

if test "x$with_io_zmq" = xyes; then
   AC_CHECK_LIB([czmq], [zctx_new], ,[AC_MSG_ERROR(
		      [CZMQ is required for the ZMQ IO module])])
fi

if test "x$with_io_kafka" = xyes; then
   # check for kafka
   AC_CHECK_LIB([rdkafka], [rd_kafka_query_watermark_offsets], ,
               [AC_MSG_ERROR( [librdkafka required for the Kafka IO module])])
fi

AC_HEADER_ASSERT

AC_CONFIG_FILES([Makefile
                lib/Makefile
                lib/consumers/Makefile
                lib/io/Makefile
                lib/io/file/Makefile
                lib/io/kafka/Makefile
                lib/io/bsrt/Makefile
                lib/io/bsrt/libbgpcorsaro/Makefile
                lib/io/bsrt/libbgpcorsaro/plugins/Makefile
                lib/io/bsrt/libbgpcorsaro/plugins/libroutingtables/Makefile
                lib/io/test/Makefile
                lib/io/zmq/Makefile
                tools/Makefile
                tools/io/Makefile
                tools/consumers/Makefile
		common/Makefile
		common/libpatricia/Makefile
		common/libinterval3/Makefile
		common/libinterval3/rb_tree/Makefile
		common/libcsv/Makefile
                common/libjsmn/Makefile])
AC_OUTPUT
