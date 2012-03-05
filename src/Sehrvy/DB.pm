# The interface to the DBI

use strict;
use warnings;


use DBI;

package Sehrvy::DB;

sub new {
  my $class = shift;
  my $self = { @_ };

  $self->{db} ||= 'dbi:mysql:sehrvy';
  $self->{user} ||= 'sehrvy_client';

  $self->{dbh} = DBI->connect($self->{db}, $self->{user}) 
    or die "Cannot connect: $DBI::errstr";

  bless $self, $class;

  $self->prepare_queries;

  $self;
}

sub prepare_queries {
  my $self = shift;

  # Vendor name from slug
  $self->{vendor_name} = $self->{dbh}->prepare(
    'SELECT vendor_name FROM Vendors WHERE vendor_slug=?');

  # Product name from product_id
  $self->{product_name} = $self->{dbh}->prepare(
    'SELECT product_name FROM Products WHERE product_id=?');

}

sub product_name {
  my ($self, $id) = @_;

  $self->{product_name}->execute($id);
  return $self->{product_name}->fetchrow_arrayref->[0];
}

sub vendor_name {
  my ($self, $slug) = @_;

  $self->{vendor_name}->execute($slug);
  my $result_ref = $self->{vendor_name}->fetchrow_arrayref;
  return defined($result_ref) ? $result_ref->[0] : 0;
}

1;

# vi:sw=2 ts=2 sts=2 et:
