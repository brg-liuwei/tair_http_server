PREFIX=/opt/tair_pkg
gcc tair_clib.cc tair_cmap.cc tair_cvec.cc -I $PREFIX/tair_bin/include -I $PREFIX/tblib_root/include/tbnet/ -I $PREFIX/tblib_root/include/tbsys/ -L $PREFIX/tair_bin/lib -L $PREFIX/tblib_root/lib -ltairclientapi -ltbsys -ltbnet -fPIC -shared -olibtair_clib.so
