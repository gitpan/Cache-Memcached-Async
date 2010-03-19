package Cache::Memcached::Async;

=head1 NAME

Cache::Memcached::Async - Asynchronous version of Cache::Memcached

=head1 SYNOPSIS

  # just like Cache::Memcached
  use Cache::Memcached::Async;
  my $mc = Cache::Memcached::Async->new(servers => [ '127.0.0.1:11211' ]);

=head1 DESCRIPTION

This is a stripped-down version of Cache::Memcached that performs gets and sets
asynchronously, notifying the caller of completion via the Danga::Socket loop.

It's used almost exactly like Cache::Memcached, except that C<get()> and C<set()>
are allowed a C<callback> parameter.

Multi-gets are not supported.

=cut

use 5.008001;

use strict;
use warnings;

no strict 'refs';
use Storable ();
use Time::HiRes ();
use String::CRC32;

use Cache::Memcached::Async::Socket;

use fields qw{
    debug no_rehash stats compress_threshold compress_enable
    readonly namespace namespace_len servers active buckets
    pref_ip
    bucketcount _single_sock _stime
    connect_timeout cb_connect_fail
    parser_class
};

# flag definitions
use constant F_STORABLE => 1;
use constant F_COMPRESS => 2;

# size savings required before saving compressed value
use constant COMPRESS_SAVINGS => 0.20; # percent

use vars qw($VERSION $HAVE_ZLIB $FLAG_NOSIGNAL);
$VERSION = "0.10_01";

BEGIN {
    $HAVE_ZLIB = eval "use Compress::Zlib (); 1;";
}

my %host_dead;   # host -> unixtime marked dead until
my %cache_sock;  # host -> socket
my @buck2sock;   # bucket number -> $sock

=head1 METHODS

For all the below methods, C<exptime> and C<timeout> are in seconds, and
C<callback> will be fired upon response from the server. C<callback> may be
undef.

C<new>, C<add>, C<replace>, C<set>, and C<delete> all inherit semantics from Cache::Memcached.

Unlike Cache::Memcached, C<incr> and C<decr> do not return the new value of the key.

=over 4

=item Cache::Memcached::Async-E<gt>new()

=cut

sub new {
    my Cache::Memcached::Async $self = shift;
    $self = fields::new( $self ) unless ref $self;

    my $args = (@_ == 1) ? shift : { @_ };  # hashref-ify args

    $self->set_servers($args->{'servers'});
    $self->{'debug'} = $args->{'debug'} || 0;
    $self->{'no_rehash'} = $args->{'no_rehash'};
    $self->{'stats'} = {};
    $self->{'pref_ip'} = $args->{'pref_ip'} || {};
    $self->{'compress_threshold'} = $args->{'compress_threshold'};
    $self->{'compress_enable'}    = 1;
    $self->{'readonly'} = $args->{'readonly'};

    # TODO: undocumented
    $self->{'connect_timeout'} = $args->{'connect_timeout'} || 0.25;
    $self->{namespace} = $args->{namespace} || '';
    $self->{namespace_len} = length $self->{namespace};

    return $self;
}

sub set_pref_ip {
    my Cache::Memcached::Async $self = shift;
    $self->{'pref_ip'} = shift;
}

sub set_servers {
    my Cache::Memcached::Async $self = shift;
    my ($list) = @_;
    $self->{'servers'} = $list || [];
    $self->{'active'} = scalar @{$self->{'servers'}};
    $self->{'buckets'} = undef;
    $self->{'bucketcount'} = 0;
    $self->init_buckets;
    @buck2sock = ();

    $self->{'_single_sock'} = undef;
    if (@{$self->{'servers'}} == 1) {
        $self->{'_single_sock'} = $self->{'servers'}[0];
    }

    return $self;
}

sub set_cb_connect_fail {
    my Cache::Memcached::Async $self = shift;
    $self->{'cb_connect_fail'} = shift;
}

sub set_connect_timeout {
    my Cache::Memcached::Async $self = shift;
    $self->{'connect_timeout'} = shift;
}

sub set_debug {
    my Cache::Memcached::Async $self = shift;
    my ($dbg) = @_;
    $self->{'debug'} = $dbg || 0;
}

sub set_readonly {
    my Cache::Memcached::Async $self = shift;
    my ($ro) = @_;
    $self->{'readonly'} = $ro;
}

sub set_norehash {
    my Cache::Memcached::Async $self = shift;
    my ($val) = @_;
    $self->{'no_rehash'} = $val;
}

sub set_compress_threshold {
    my Cache::Memcached::Async $self = shift;
    my ($thresh) = @_;
    $self->{'compress_threshold'} = $thresh;
}

sub enable_compress {
    my Cache::Memcached::Async $self = shift;
    my ($enable) = @_;
    $self->{'compress_enable'} = $enable;
}

sub forget_dead_hosts {
    %host_dead = ();
    @buck2sock = ();
}

my %sock_map;  # stringified-$sock -> "$ip:$port"

sub _dead_sock {
    my ($sock, $ret, $dead_for) = @_;
    if (my $ipport = $sock_map{$sock}) {
        my $now = time();
        $host_dead{$ipport} = $now + $dead_for
            if $dead_for;
        delete $cache_sock{$ipport};
        delete $sock_map{$sock};
    }
    @buck2sock = ();
    return $ret;  # 0 or undef, probably, depending on what caller wants
}

sub _close_sock {
    my ($sock) = @_;
    if (my $ipport = $sock_map{$sock}) {
        close $sock;
        delete $cache_sock{$ipport};
        delete $sock_map{$sock};
    }
    @buck2sock = ();
}

sub sock_to_host { # (host)
    my Cache::Memcached::Async $self = shift;
    my $host = shift;
    return $cache_sock{$host} if $cache_sock{$host} && !$cache_sock{$host}{closed};

    my $sock = Cache::Memcached::Async::Socket->new($host);

    $cache_sock{$host} = $sock;
    $sock_map{$sock} = $host;

    return $sock;
}

sub get_sock { # (key)
    my Cache::Memcached::Async $self = $_[0];
    my $key = $_[1];
    return $self->sock_to_host($self->{'_single_sock'}) if $self->{'_single_sock'};
    return undef unless $self->{'active'};
    my $hv = ref $key ? int($key->[0]) : _hashfunc($key);

    my $real_key = ref $key ? $key->[1] : $key;
    my $tries = 0;
    while ($tries++ < 20) {
        my $host = $self->{'buckets'}->[$hv % $self->{'bucketcount'}];
        my $sock = $self->sock_to_host($host);
        return $sock if $sock;
        return undef if $self->{'no_rehash'};
        $hv += _hashfunc($tries . $real_key);  # stupid, but works
    }
    return undef;
}

sub init_buckets {
    my Cache::Memcached::Async $self = shift;
    return if $self->{'buckets'};
    my $bu = $self->{'buckets'} = [];
    foreach my $v (@{$self->{'servers'}}) {
        if (ref $v eq "ARRAY") {
            for (1..$v->[1]) { push @$bu, $v->[0]; }
        } else {
            push @$bu, $v;
        }
    }
    $self->{'bucketcount'} = scalar @{$self->{'buckets'}};
}

sub disconnect_all {
    my $sock;
    foreach $sock (values %cache_sock) {
        close $sock;
    }
    %cache_sock = ();
}

=item $mc->delete($key, timeout => $timeout_seconds, callback => \&callback);

=cut

sub delete {
    my Cache::Memcached::Async $self = shift;
    my ($key, %opts) = @_;

    my $sock = $self->get_sock($key);
    return 0 unless $sock;

    $key = ref $key ? $key->[1] : $key;
    my $time = $opts{time};
    $time = $time ? " $time" : "";
    my $cmd = "delete $self->{namespace}$key$time\r\n";
    $sock->run($cmd, $opts{callback}, undef, $opts{timeout});

    return 1;
}
*remove = \&delete;

=item $mc->add($key, $value, exptime => $expiration, timeout => $timeout, callback => \&callback);

=cut

sub add {
    _set("add", @_);
}

=item $mc->replace($key, $value, exptime => $expiration_seconds, timeout => $timeout_seconds, callback => \&callback);

=cut

sub replace {
    _set("replace", @_);
}

=item $mc->set($key, $value, exptime => $expiration_seconds, timeout => $timeout_seconds, callback => \&callback);

=cut

sub set {
    _set("set", @_);
}

sub _set {
    my $cmdname = shift;
    my Cache::Memcached::Async $self = shift;
    my ($key, $val, %opts) = @_;
    my $sock = $self->get_sock($key);
    return 0 unless $sock;

    use bytes; # return bytes from length()

    my $flags = 0;
    $key = ref $key ? $key->[1] : $key;

    if (ref $val) {
        local $Carp::CarpLevel = 2;
        $val = Storable::nfreeze($val);
        $flags |= F_STORABLE;
    }

    my $len = length($val);

    if ($self->{'compress_threshold'} && $HAVE_ZLIB && $self->{'compress_enable'} &&
        $len >= $self->{'compress_threshold'}) {

        my $c_val = Compress::Zlib::memGzip($val);
        my $c_len = length($c_val);

        # do we want to keep it?
        if ($c_len < $len*(1 - COMPRESS_SAVINGS)) {
            $val = $c_val;
            $len = $c_len;
            $flags |= F_COMPRESS;
        }
    }

    my $exptime = $opts{exptime};
    $exptime = int($exptime || 0);

    my $line = "$cmdname $self->{namespace}$key $flags $exptime $len\r\n$val\r\n";

    my $res = $sock->run($line, $opts{callback}, undef, $opts{timeout});

    return 1;
}

=item $mc->incr($key, $step, timeout => $timeout_seconds, callback => \&callback);

=cut

sub incr {
    _incrdecr("incr", @_);
}

=item $mc->decr($key, $step, timeout => $timeout_seconds, callback => \&callback);

=cut

sub decr {
    _incrdecr("decr", @_);
}

sub _incrdecr {
    my $cmdname = shift;
    my Cache::Memcached::Async $self = shift;
    my ($key, $value, %opts) = @_;
    my $sock = $self->get_sock($key);
    return undef unless $sock;
    $key = $key->[1] if ref $key;
    $value = 1 unless defined $value;

    my $line = "$cmdname $self->{namespace}$key $value\r\n";
    $sock->run($line, $opts{callback}, undef, $opts{timeout});

    return 1;
}

=item $mc->get($key, timeout => $timeout_seconds, callback => \&callback);

For C<get()>, C<callback> is passed the cached value on hit, or undef on miss.

=back

=cut

sub get {
    my Cache::Memcached::Async $self = shift;
    my ($get_key, %opts) = @_;
    my $sock = $self->get_sock($get_key);

    my ($key, $flags, $length);

    my $parser = sub {
        my ($bufref, $callback) = @_;

        while (1) {
            my $called_back = 0;
            if (defined $length) {
                # Yes, that's right, we have to read an extra two bytes because memcached is acting like a line server.
                return unless length($$bufref) >= $length + 2;

                my $value = substr $$bufref, 0, $length, '';
                my $crlf = substr $$bufref, 0, 2, '';
                unless ($crlf eq "\r\n") {
                    $crlf =~ s/(\W)/quotemeta $1/ge;
                    die "$self I expected a CR LF pair here, instead I got crlf=$crlf\n";
                }

                undef $length;
                if ($callback) {
                    $callback->($value);
                    $called_back = 1;
                }
            }

            if ($$bufref =~ s/^VALUE (\S+) (\d+) (\d+)\r\n//) {
                # State: 'VALUE' line received, loop back and try to read the data block
                $key = $1;
                $flags = $2;
                $length = $3;
                next;
            }

            if ($$bufref =~ s/^END\r\n//) {
                # State: 'END\r\n' recieved, we can return and say we're done.
                $callback->() if $callback && !$called_back;
                return 1;
            }

            # State: still waiting for END or another VALUE
            return;
        }
    };

    my $line = "get $self->{namespace}$get_key\r\n";

    $sock->run($line, $opts{callback}, $parser, $opts{timeout});

    return 1;
}

sub _hashfunc {
    return (crc32($_[0]) >> 16) & 0x7fff;
}

1;

__END__

=head1 SEE ALSO

Cache::Memcached

=head1 AUTHOR

Jonathan Steinert E<lt>hachi@cpan.orgE<gt>

Adam Thomason E<lt>athomason@sixapart.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 by Six Apart, Ltd.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.5 or,
at your option, any later version of Perl 5 you may have available.

=cut
