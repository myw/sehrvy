#!/usr/bin/perl -w
# Trivial script only intented to test connection. If you have a username and password,
# run this script as follows:
#
#    DBI_USER=<your_username> DBI_PASS=<your_password> ./test_db.pl
#
# If no error message is printed, you're golden.

use strict;
use warnings;
use DBI;
use 5.008;

MAIN: {
  my $dbh;

  $dbh = DBI->connect('dbi:mysql:sehrvy', 'sehrvy_client') or die "Cannot connect as sehrvy_client: $DBI::errstr";
  $dbh->disconnect;

  $dbh = DBI->connect('dbi:mysql:sehrvy', 'sehrvy_admin') or die "Cannot connect as sehrvy_admin: $DBI::errstr";
  $dbh->disconnect;
}

# vi:sw=2 ts=2 sts=2 et:
