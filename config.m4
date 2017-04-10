dnl $Id$
dnl config.m4 for extension beanstalkd

dnl Comments in this file start with the string 'dnl'.
dnl Remove where necessary. This file will not work
dnl without editing.

dnl If your extension references something external, use with:

 PHP_ARG_WITH(beanstalkd, for beanstalkd support,
 Make sure that the comment is aligned:
 [  --with-beanstalkd             Include beanstalkd support])

dnl Otherwise use enable:

dnl PHP_ARG_ENABLE(beanstalkd, whether to enable beanstalkd support,
dnl Make sure that the comment is aligned:
dnl [  --enable-beanstalkd           Enable beanstalkd support])

if test "$PHP_BEANSTALKD" != "no"; then
  dnl Write more examples of tests here...

  dnl # --with-beanstalkd -> check with-path
  dnl SEARCH_PATH="/usr/local /usr"     # you might want to change this
  dnl SEARCH_FOR="/include/beanstalkd.h"  # you most likely want to change this
  dnl if test -r $PHP_BEANSTALKD/$SEARCH_FOR; then # path given as parameter
  dnl   BEANSTALKD_DIR=$PHP_BEANSTALKD
  dnl else # search default path list
  dnl   AC_MSG_CHECKING([for beanstalkd files in default path])
  dnl   for i in $SEARCH_PATH ; do
  dnl     if test -r $i/$SEARCH_FOR; then
  dnl       BEANSTALKD_DIR=$i
  dnl       AC_MSG_RESULT(found in $i)
  dnl     fi
  dnl   done
  dnl fi
  dnl
  dnl if test -z "$BEANSTALKD_DIR"; then
  dnl   AC_MSG_RESULT([not found])
  dnl   AC_MSG_ERROR([Please reinstall the beanstalkd distribution])
  dnl fi

  dnl # --with-beanstalkd -> add include path
  dnl PHP_ADD_INCLUDE($BEANSTALKD_DIR/include)

  dnl # --with-beanstalkd -> check for lib and symbol presence
  dnl LIBNAME=beanstalkd # you may want to change this
  dnl LIBSYMBOL=beanstalkd # you most likely want to change this 

  dnl PHP_CHECK_LIBRARY($LIBNAME,$LIBSYMBOL,
  dnl [
  dnl   PHP_ADD_LIBRARY_WITH_PATH($LIBNAME, $BEANSTALKD_DIR/$PHP_LIBDIR, BEANSTALKD_SHARED_LIBADD)
  dnl   AC_DEFINE(HAVE_BEANSTALKDLIB,1,[ ])
  dnl ],[
  dnl   AC_MSG_ERROR([wrong beanstalkd lib version or lib not found])
  dnl ],[
  dnl   -L$BEANSTALKD_DIR/$PHP_LIBDIR -lm
  dnl ])
  dnl
  dnl PHP_SUBST(BEANSTALKD_SHARED_LIBADD)

  PHP_NEW_EXTENSION(beanstalkd, beanstalkd.c  beanstalkd_standard_hash.c beanstalkd_consistent_hash.c, $ext_shared)
fi
