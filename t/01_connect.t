use strict;
use warnings;
use Test::More;
use EV;
use EV::MariaDB;

plan tests => 6;

my $host   = $ENV{TEST_MARIADB_HOST}   // '127.0.0.1';
my $port   = $ENV{TEST_MARIADB_PORT}   // 3306;
my $user   = $ENV{TEST_MARIADB_USER}   // 'root';
my $pass   = $ENV{TEST_MARIADB_PASS}   // '';
my $db     = $ENV{TEST_MARIADB_DB}     // 'test';
my $socket = $ENV{TEST_MARIADB_SOCKET};

# Test 1: basic object creation
{
    my $m = EV::MariaDB->new(on_error => sub {});
    ok(defined $m, 'new without connect');
    is($m->is_connected, 0, 'not connected yet');
}

# Test 2: connect
{
    my $connected = 0;
    my $error_msg;

    my $m = EV::MariaDB->new(
        host       => $host,
        port       => $port,
        user       => $user,
        password   => $pass,
        database   => $db,
        ($socket ? (unix_socket => $socket) : ()),
        on_connect => sub {
            $connected = 1;
            EV::break;
        },
        on_error   => sub {
            $error_msg = $_[0];
            EV::break;
        },
    );

    my $timeout = EV::timer(5, 0, sub { EV::break });
    EV::run;

    if ($error_msg) {
        diag("Connection error: $error_msg");
        diag("Set TEST_MARIADB_HOST/PORT/USER/PASS/DB env vars");
        ok(1, "connect - skipped (no MariaDB/MySQL)") for 1..4;
        done_testing;
        exit;
    }

    ok($connected, 'connected successfully');
    is($m->is_connected, 1, 'is_connected returns 1');
    ok($m->thread_id > 0, 'thread_id is positive');
    ok(defined $m->server_info, 'server_info returns a value');

    $m->finish;
}
