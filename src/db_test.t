#!/usr/bin/env perl
use strict;
use warnings;
use Test::Simple tests => 2;
use Sehrvy::DB;
use 5.008;


my @slugs = qw(scc athenahealth scc pal xcite);
my $db = Sehrvy::DB->new;

ok( defined($db) , 'Database handle exists');
ok ( $db->vendor_name('scc') eq 'SCC Soft Computer','Slug resolved to name');

sub test_something{
  #
  return 0;
}

#foreach(@slugs){
#  sleep 2;
#  my $name = $db->vendor_name($_);
#  print "Slug=$_\n" . "Name=$name\n";
#}
#
# vi:sw=2 ts=2 sts=2 et:
