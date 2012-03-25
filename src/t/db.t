#!/usr/bin/env perl
use strict;
use warnings;
use Test::Simple tests => 5;
use Sehrvy::DB;
use 5.008;


my @slugs = qw(scc athenahealth pal xcite);
my @full_names = (
  'SCC Soft Computer', 
  'athenahealth, Inc',
  'PAL/MED Development, LLC',
  'Xcite Health Corp. and Encounterpro Healthcare Resources Inc.'
);

my $db = Sehrvy::DB->new;
ok(defined($db) , 'Database handle exists');


for (0 .. $#slugs) {
  my $sl = $slugs[$_];
  my $fn = $full_names[$_];

  ok($db->vendor_name($sl) eq $fn, "slug [$sl] resolved to [$fn]");
}

# vi:sw=2 ts=2 sts=2 et:
