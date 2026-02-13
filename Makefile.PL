use strict;
use warnings;
use ExtUtils::MakeMaker;
use EV::MakeMaker;

my $mc = $ENV{MARIADB_CONFIG} || 'mariadb_config';

my $mc_incdir = `$mc --include 2>/dev/null`;
chomp $mc_incdir;
if (!$mc_incdir) {
    $mc = 'mysql_config';
    $mc_incdir = `$mc --include 2>/dev/null`;
    chomp $mc_incdir;
}
die "Cannot find mariadb_config or mysql_config. Install libmariadb-dev or set MARIADB_CONFIG env var.\n"
    unless $mc_incdir;

my $mc_libs = `$mc --libs 2>/dev/null`;
chomp $mc_libs;

my %ev = EV::MakeMaker::ev_args();

WriteMakefile(
    NAME          => 'EV::MariaDB',
    VERSION_FROM  => 'lib/EV/MariaDB.pm',
    ABSTRACT      => 'Async MariaDB/MySQL client using libmariadb and EV',
    AUTHOR        => 'Yury Kotlyarov',
    LICENSE       => 'perl_5',
    MIN_PERL_VERSION => '5.012',
    PREREQ_PM     => { 'EV' => '4.11' },
    XS            => { 'MariaDB.xs' => 'MariaDB.c' },
    INC           => "$mc_incdir $ev{INC}",
    LIBS          => [$mc_libs],
    OBJECT        => 'MariaDB$(OBJ_EXT)',
    CONFIGURE_REQUIRES => {
        'EV::MakeMaker'       => 0,
        'ExtUtils::MakeMaker' => '6.64',
    },
    TEST_REQUIRES => {
        'Test::More' => '0.98',
    },
    META_MERGE => {
        'meta-spec' => { version => 2 },
    },
);
