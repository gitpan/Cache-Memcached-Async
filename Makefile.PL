use inc::Module::Install;

name           'Cache-Memcached-Async';
all_from       'lib/Cache/Memcached/Async.pm';

requires 'Danga::Socket' => 1.59;


WriteAll;
