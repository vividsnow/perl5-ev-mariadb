use strict;
use warnings;
use Test::More;
use EV;
use EV::MariaDB;

plan tests => 21;

my $socket = $ENV{TEST_MARIADB_SOCKET};
my $host   = $ENV{TEST_MARIADB_HOST}   // ($socket ? 'localhost' : '127.0.0.1');
my $port   = $ENV{TEST_MARIADB_PORT}   // 3306;
my $user   = $ENV{TEST_MARIADB_USER}   // 'root';
my $pass   = $ENV{TEST_MARIADB_PASS}   // '';
my $db     = $ENV{TEST_MARIADB_DB}     // 'test';

my $m;

sub with_mariadb {
    my (%args) = @_;
    my $cb = delete $args{cb};
    $m = EV::MariaDB->new(
        host       => $host,
        port       => $port,
        user       => $user,
        password   => $pass,
        database   => $db,
        ($socket ? (unix_socket => $socket) : ()),
        on_connect => sub { $cb->() },
        on_error   => sub {
            diag("Error: $_[0]");
            EV::break;
        },
        %args,
    );
    my $timeout = EV::timer(10, 0, sub { EV::break });
    EV::run;
    $m->finish if $m && $m->is_connected;
}

# Test 1-3: stmt truncation refetch (MYSQL_DATA_TRUNCATED path)
with_mariadb(cb => sub {
    my $long_str = 'x' x 1000;
    $m->prepare("select ?", sub {
        my ($stmt, $err) = @_;
        die "prepare: $err" if $err;
        $m->execute($stmt, [$long_str], sub {
            my ($rows, $err2) = @_;
            ok(!$err2, 'truncation refetch: no error');
            is(scalar @$rows, 1, 'truncation refetch: 1 row');
            is($rows->[0][0], $long_str, 'truncation refetch: data matches');
            $m->close_stmt($stmt, sub { EV::break });
        });
    });
});

# Test 4-5: stmt with multiple columns of varying sizes
with_mariadb(cb => sub {
    $m->prepare("select ?, ?, ?", sub {
        my ($stmt, $err) = @_;
        die "prepare: $err" if $err;
        my $big = 'y' x 2000;
        $m->execute($stmt, ['short', $big, 'also_short'], sub {
            my ($rows, $err2) = @_;
            ok(!$err2, 'multi-col truncation: no error');
            is_deeply($rows->[0], ['short', $big, 'also_short'],
                'multi-col truncation: all columns correct');
            $m->close_stmt($stmt, sub { EV::break });
        });
    });
});

# Test 6-8: reset() with queued operations
with_mariadb(cb => sub {
    my @results;
    for my $i (1..3) {
        $m->q("select $i", sub {
            my ($rows, $err) = @_;
            push @results, $err ? "err:$err" : "ok:$rows->[0][0]";
        });
    }
    # reset cancels all pending, reconnects
    $m->reset;
    is(scalar @results, 3, 'reset with queue: all 3 callbacks fired');
    is(scalar(grep { /^err:/ } @results), 3, 'reset with queue: all got errors');

    # after reset, on_connect fires again — queue a verification query
    my $reconnected = 0;
    $m->on_connect(sub {
        $reconnected = 1;
        $m->q("select 'after_reset'", sub {
            my ($rows, $err) = @_;
            is($rows->[0][0], 'after_reset', 'reset with queue: query after reconnect works');
            EV::break;
        });
    });
});

# Test 9-11: DESTROY during active async operation (deferred free)
{
    my $destroyed_ok = 1;
    my $cb_fired = 0;
    {
        my $obj;
        $obj = EV::MariaDB->new(
            host       => $host,
            port       => $port,
            user       => $user,
            password   => $pass,
            database   => $db,
            ($socket ? (unix_socket => $socket) : ()),
            on_connect => sub {
                $obj->q("select 1", sub {
                    $cb_fired = 1;
                });
                # let obj go out of scope while query is pending
                undef $obj;
            },
            on_error => sub {
                $cb_fired = 1;
            },
        );
        my $timeout = EV::timer(5, 0, sub { EV::break });
        EV::run;
    }
    ok($destroyed_ok, 'DESTROY during active: no crash');
    ok(1, 'DESTROY during active: survived');
    # The query callback should have fired (either result or error from destroy)
    ok($cb_fired, 'DESTROY during active: callback fired');
}

# Test 12-13: deferred connect via connect()
{
    my $obj = EV::MariaDB->new(
        on_connect => sub { },
        on_error   => sub { diag("Error: $_[0]") },
    );
    ok(!$obj->is_connected, 'deferred connect: not connected initially');
    $obj->on_connect(sub {
        ok($obj->is_connected, 'deferred connect: connected after connect()');
        EV::break;
    });
    $obj->connect($host, $user, $pass, $db, $port,
        ($socket ? $socket : undef));
    my $timeout = EV::timer(5, 0, sub { EV::break });
    EV::run;
    $obj->finish if $obj->is_connected;
}

# Test 14-15: init_command option
with_mariadb(
    init_command => "set \@ev_mariadb_init = 42",
    cb => sub {
        $m->q("select \@ev_mariadb_init", sub {
            my ($rows, $err) = @_;
            ok(!$err, 'init_command: no error');
            is($rows->[0][0], 42, 'init_command: variable set by init_command');
            EV::break;
        });
    },
);

# Test 16-17: charset option
with_mariadb(
    charset => 'utf8mb4',
    cb => sub {
        ok($m->character_set_name =~ /utf8mb4/i, 'charset option: utf8mb4 set');
        $m->q("select '✓'", sub {
            my ($rows, $err) = @_;
            ok(!$err, 'charset option: utf8mb4 query works');
            EV::break;
        });
    },
);

# Test 18: skip_pending after reset (no double-free / crash)
with_mariadb(cb => sub {
    $m->q("select 1", sub { });
    $m->q("select 2", sub { });
    $m->skip_pending;
    $m->q("select 'survived'", sub {
        my ($rows, $err) = @_;
        is($rows->[0][0], 'survived', 'skip_pending then query: works');
        EV::break;
    });
});

# Test 19: prepare() from pipeline callback with pending results should croak
with_mariadb(cb => sub {
    my $croak_msg;
    $m->q("select 1", sub {
        $m->q("select 2", sub {
            # send_count=1 here (Q3 result still pending)
            eval { $m->prepare("select ?", sub { }) };
            $croak_msg = $@;
        });
        $m->q("select 3", sub {
            like($croak_msg, qr/pipeline/, 'prepare during pipeline: croaks');
            EV::break;
        });
    });
});

# Test 20: ping() from pipeline callback with pending results should croak
with_mariadb(cb => sub {
    my $croak_msg;
    $m->q("select 1", sub {
        $m->q("select 2", sub {
            eval { $m->ping(sub { }) };
            $croak_msg = $@;
        });
        $m->q("select 3", sub {
            like($croak_msg, qr/pipeline/, 'ping during pipeline: croaks');
            EV::break;
        });
    });
});

# Test 21: skip_pending from pipeline callback cancels remaining, disconnects
with_mariadb(cb => sub {
    my @results;
    $m->q("select 1", sub {
        $m->q("select 2", sub {
            push @results, 'q2';
            $m->skip_pending;  # Q3's cb fires synchronously here
            push @results, 'disconnected' unless $m->is_connected;
            is_deeply(\@results, ['q2', 'q3_err', 'disconnected'],
                'skip_pending from pipeline: cancels remaining, disconnects');
            EV::break;
        });
        $m->q("select 3", sub {
            my ($rows, $err) = @_;
            push @results, $err ? 'q3_err' : 'q3_ok';
        });
    });
});
