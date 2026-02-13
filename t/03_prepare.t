use strict;
use warnings;
use Test::More;
use EV;
use EV::MariaDB;

plan tests => 7;

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

# prepare + execute
with_mariadb(sub {
    $m->prepare("select ? + ? as sum", sub {
        my ($stmt, $err) = @_;
        ok(!$err, 'prepare: no error');
        ok(defined $stmt, 'prepare: got stmt handle');

        $m->execute($stmt, [10, 20], sub {
            my ($rows, $err2) = @_;
            ok(!$err2, 'execute: no error');
            is($rows->[0][0], '30', 'execute: 10+20=30');

            $m->close_stmt($stmt, sub {
                my ($ok, $err3) = @_;
                ok(!$err3, 'close_stmt: no error');
                EV::break;
            });
        });
    });
});

# execute with null param
with_mariadb(sub {
    $m->prepare("select ? is null as isnull", sub {
        my ($stmt, $err) = @_;
        ok(!$err, 'prepare null test');

        $m->execute($stmt, [undef], sub {
            my ($rows, $err2) = @_;
            is($rows->[0][0], '1', 'null param: is null is 1');

            $m->close_stmt($stmt, sub { EV::break });
        });
    });
});
