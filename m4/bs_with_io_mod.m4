# SYNOPSIS
#
#   BS_WITH_IO_MOD
#
# LICENSE
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

AC_DEFUN([BS_WITH_IO_MOD],
[
	AC_MSG_CHECKING([whether to build with BGPView IO module: $1])

        ifelse([$3], [yes],
            [AC_ARG_WITH([$1-io],
              [AS_HELP_STRING([--without-$1-io],
                [build without the $1 IO module])],
              [with_io_$1=$with_$1_io],
              [with_io_$1=yes])],
            [AC_ARG_WITH([$1-io],
              [AS_HELP_STRING([--with-$1-io],
                [build with the $1 IO module])],
              [with_io_$1=$with_$1_io],
              [with_io_$1=no])]
            )

        WITH_BGPVIEW_IO_$2=
        AS_IF([test "x$with_io_$1" != xno],
              [
		AC_DEFINE([WITH_BGPVIEW_IO_$2],[1],[Building with $1 IO])
	      ])
	AC_MSG_RESULT([$with_io_$1])

	AM_CONDITIONAL([WITH_BGPVIEW_IO_$2], [test "x$with_io_$1" != xno])
])
