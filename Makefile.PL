use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME          => 'MySQL::QueryMulti',
    AUTHOR        => q{John Gravatt <gravattj@cpan.org>},
    VERSION_FROM  => 'lib/MySQL/QueryMulti.pm',
    ABSTRACT_FROM => 'lib/MySQL/QueryMulti.pm',
    ( $ExtUtils::MakeMaker::VERSION >= 6.3002
        ? ( 'LICENSE' => 'perl' )
        : () ),
    PL_FILES  => {},
    PREREQ_PM => {
        'Test::More'       => 0,
        'SQL::Statement'   => 1.33,
        'DBI'              => 1.6,
        'DBD::mysql'       => 4.019,
        'Moose'            => 2.0602,
        'Text::ASCIITable' => 0.18,
        'Term::ReadLine'   => 1.07,
        'Time::HiRes'      => 0,

    },
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES    => 'MySQL-QueryMulti-*' },
);
