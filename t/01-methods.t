#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use Data::Dumper;
use MySQL::QueryMulti;
use Data::Compare;

#####################

my ( $sth, $cnt, $comp, $cmd, $pass, $qm, $sql );

read_conf();

$cmd = get_mysql_cmd() . " < t/sql";
system($cmd);
die if $?;

$pass = defined( $ENV{DBI_PASS} ) ? $ENV{DBI_PASS} : undef;

$qm = get_new_qm_obj();

#
# test a basic query
#
ok( $qm->prepare("select * from pet") );

# group by birth order by birth, owner limit 1"));

$sth = $qm->execute;

$cnt = 0;
while ( my $href = $sth->fetchrow_hashref ) {
    $cnt++;
}

ok( $cnt == 9 );

#
# test a query with limit
#
ok( $qm->prepare("select * from pet limit 1") );
$sth = $qm->execute;

$cnt = 0;
while ( my $href = $sth->fetchrow_hashref ) {
    $cnt++;
}

ok( $cnt == 1 );

#
# test a query with order by
#
ok( $qm->prepare("select birth from pet order by birth") );
$sth = $qm->execute;

my @expected = (
    '1979-08-31', '1989-05-13', '1990-08-27', '1993-02-04',
    '1994-03-17', '1996-04-29', '1997-12-09', '1998-09-11',
    '1999-03-30'
);
my @actual = ();

while ( my $href = $sth->fetchrow_hashref ) {
    push( @actual, $href->{birth} );
}

$comp = Data::Compare->new;
ok( $comp->Cmp( \@expected, \@actual ) );

#
# test a query with group by
#
$sql = q{
    select species, min(age) as age 
    from pet p, owner o
    where p.owner = o.owner 
    group by species
};

ok( $qm->prepare($sql) );
ok( $sth = $qm->execute );

my %expected = (
    dog     => 5,
    cat     => 5,
    hamster => 5,
    snake   => 5,
    bird    => 5
);

my %actual = ();

while ( my $href = $sth->fetchrow_hashref ) {
    $actual{ $href->{species} } = $href->{age};
}

$comp = Data::Compare->new;
ok( $comp->Cmp( \%expected, \%actual ) );

#
# test select distinct
#
$sql = q{
    select distinct species
    from pet
    order by species
};
ok( $qm->prepare($sql) );
ok( $sth = $qm->execute() );

@expected = qw(bird cat dog hamster snake);
@actual = ();

while ( my ($species) = $sth->fetchrow_array ) {
    push( @actual, $species );
}

$comp = Data::Compare->new;
ok( $comp->Cmp( \@expected, \@actual ) );

#
# test with placeholders
#
$sql = q{
    select distinct species 
    from pet 
    where sex = ?
    order by species
};

ok( $qm->prepare($sql) );
ok( $sth = $qm->execute('m') );

@expected = ( 'cat', 'dog', 'snake' );
@actual = ();

while ( my ($species) = $sth->fetchrow_array ) {
    push( @actual, $species );
}

$comp = Data::Compare->new;
ok( $comp->Cmp( \@expected, \@actual ) );

#
# try a bad clause
#
eval { $qm->prepare("selec * from pet") };
ok($@);

#
# try a bad table
#
ok( $qm->prepare("select * from bogus") );
ok($@);

#
# try a bad query twice.  there was a bug that wasn't cleaning up sth's
# properly when this occurred
#
eval { $qm->prepare("select id from pet") };
ok( !$@ );
eval { $qm->execute; };
my $error1 = get_first_line($@);
ok($@);

eval { $qm->prepare("select id from pet") };
ok( !$@ );
eval { $qm->execute };
my $error2 = get_first_line($@);
ok($@);

ok( $error1 eq $error2 );

#
# done!
#
done_testing();

########################

sub get_first_line {
    my $str = shift;
    
    return (split(/\n/, $str))[0];    
}

sub get_new_qm_obj {
    my $qm = MySQL::QueryMulti->new;
    ok($qm);
    
    #
    # test the connect method
    #
    ok( $qm->connect(

            # ($data_source, $username, $password, \%attr)
            [ get_dsn('pet1'), $ENV{DBI_USER}, $pass ],
            [ get_dsn('pet2'), $ENV{DBI_USER}, $pass ],
        )
    );

    return $qm;
}

END {

    # cleanup
    my $cmd = get_mysql_cmd() . q{ -e "drop database if exists pet1"};
    system($cmd);
    die if $?;

    $cmd = get_mysql_cmd() . q{ -e "drop database if exists pet2"};
    system($cmd);
    die if $?;
}

sub get_dsn {
    my $db = shift;

    my $dsn = $ENV{DBI_DSN};
    if ( $dsn !~ /\;\s*$/ ) {
        $dsn .= ";";
    }

    $dsn .= "database=$db";

    return $dsn;
}

sub get_mysql_cmd {
    my $cmd = "mysql -u $ENV{DBI_USER} -h $ENV{MYSQL_HOST} ";
    $cmd .= '-p$Pass ' if defined( $ENV{DBI_PASS} );

    return $cmd;
}

sub read_conf {
    open( IN, 'CONF' ) or die "failed to open CONF: $!";

    while (<IN>) {
        next if /^\s*$/;
        next if !/=/;
        chomp;
        m/(.*?)\s*=\s*(.*)/;
        $ENV{$1} = $2;
    }

    close(IN);
}
