package Cache::Memcached::Async::Socket;

use Danga::Socket;
use IO::Socket::INET;

use base 'Danga::Socket';
use fields qw(buf handlers connect_callback);

sub new {
    my Cache::Memcached::Async::Socket $self = shift;
    my $addr = shift;
    my %opts = @_;

    my $sock = IO::Socket::INET->new(PeerAddr => $addr, Blocking => 0);

    $self = fields::new($self) unless ref $self;

    $self->SUPER::new($sock);

    $self->{handlers} = [];
    $self->{connect_callback} = $opts{callback};
    $self->{buf} = '';

    $self->watch_write(1);

    return $self;
}

sub event_write {
    my Cache::Memcached::Async::Socket $self = shift;

    my $connect_callback = $self->{connect_callback};

    $connect_callback->() if $connect_callback;

    $self->watch_write(0);

    return;
}

sub line_parser {
    my ($bufref, $callback) = @_;
    if ($$bufref =~ s/^(.*?)\r\n//) {
        $callback->($1);
        return 1;
    }
    return;
}

sub event_read {
    my Cache::Memcached::Async::Socket $self = shift;

    my $readref = $self->read(65536);

    return unless defined $readref && defined $$readref;

    my $bufref = \$self->{buf};
    my $handlers = $self->{handlers};

    $$bufref .= $$readref;

    while (@$handlers) {
        my $parser = $handlers->[0]->{parser} || \&line_parser;
        my $callback = $handlers->[0]->{callback};
        my $rv = $parser->($bufref, $callback);

        # If the last parser returned false, then we've got an incomplete run on that parser so far.
        last unless $rv;

        # If we get here, then the previous parser has finished and is ready to move on.
        shift @$handlers;
    }

    $self->watch_read(0) unless @$handlers;
}

sub handle_error {
    my Cache::Memcached::Async::Socket $self = shift;

    # on errors, let clients know they're not going to get anything back
    my $handlers = $self->{handlers};
    while (@$handlers) {
        my $handler = shift @$handlers;
        my $callback = $handler->{callback};
        $callback->() if $callback;
    }
}

sub event_err {
    shift->handle_error(@_);
}

sub event_hup {
    shift->handle_error(@_);
}

sub close {
    my $self = shift;
    $self->handle_error();
    $self->SUPER::close(@_);
}

sub run {
    my Cache::Memcached::Async::Socket $self = shift;
    my $command = shift;
    my $callback = shift;
    my $parser = shift;
    my $timeout = shift;

    my $handlers = $self->{handlers};

    {
        local $SIG{PIPE} = "IGNORE";
        $self->write($command);
    }

    $self->watch_read(1);

    my $timer;
    if ($timeout) {
        $timer = Danga::Socket->AddTimer($timeout, sub { $self->close });
    }

    push @$handlers, {
        callback => sub {
            $timer->cancel() if $timer;
            $callback->(@_) if $callback;
        },
        parser => $parser,
    };
}

1;
