gcc tair_clib.cc -I/root/tair_bin/include -I/tblib_root/include/tbnet/ -I/tblib_root/include/tbsys/ -L/root/tair_bin/lib -L/tblib_root/lib -ltairclientapi -ltbsys -ltbnet -fPIC -shared -olibtair_clib.so
