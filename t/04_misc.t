use strict;
use warnings;
use Test::More;
use EV;
use EV::MariaDB;

plan tests => 9;

my $host   = $ENV{TEST_MARIADB_HOST}   // '127.0.0.1';
my $port   = $ENV{TEST_MARIADB_PORT}   // 3306;
my $user   = $ENV{TEST_MARIADB_USER}   // 'root';
my $pass   = $ENV{TEST_MARIADB_PASS}   // '';
my $db     = $ENV{TEST_MARIADB_DB}     // 'test';
my $socket = $ENV{TEST_MARIADB_SOCKET};

my $m;

sub with_mariadb {
    my ($cb) = @_;
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
    );
    my $timeout = EV::timer(5, 0, sub { EV::break });
    EV::run;
    $m->finish if $m->is_connected;
}

# escape
with_mariadb(sub {
    my $escaped = $m->escape("it's a test");
    like($escaped, qr/it\\'s a test|it''s a test/, 'escape: quote escaped');
    is($m->escape("hello"), 'hello', 'escape: no special chars');

    # accessors
    ok($m->server_version > 0, 'server_version is positive');
    ok(defined $m->host_info, 'host_info returns a value');
    ok(defined $m->character_set_name, 'character_set_name returns a value');
    ok($m->socket >= 0, 'socket returns valid fd');

    EV::break;
});

# error on invalid query
with_mariadb(sub {
    $m->query("invalid sql gibberish", sub {
        my ($rows, $err) = @_;
        ok($err, 'invalid query: got error');
        ok(!defined $rows || !ref $rows, 'invalid query: no rows');
        EV::break;
    });
});

# reset
with_mariadb(sub {
    $m->on_connect(sub {
        ok($m->is_connected, 'reset: reconnected');
        EV::break;
    });
    $m->reset;
});
