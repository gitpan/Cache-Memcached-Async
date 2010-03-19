use Test::More tests => 3;
use Danga::Socket;

BEGIN { use_ok('Cache::Memcached::Async') };

my $mc = Cache::Memcached::Async->new(servers => [ '127.0.0.1:11211' ]);

ok(defined $mc, 'new()');

sub go {
    my $result;
    $mc->set('foo' => 'bar', callback => sub { $result .= "set returned: @_\n" });
    $mc->get('foo', callback => sub { $result .= "get returned: @_\n"; die $result });
    Danga::Socket->AddTimer(0, \&go);
}

Danga::Socket->AddTimer(0, \&go);
eval { Danga::Socket->EventLoop; };
is($@, "set returned: STORED\nget returned: bar\n", 'set/get');
