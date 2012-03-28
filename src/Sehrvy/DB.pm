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

  $self->{dbh} = DBI->connect($self->{db}, $self->{user}, undef,
                              {AutoInactiveDestroy => 1})
    or die "Cannot connect: $DBI::errstr";

  bless $self, $class;

  $self->_prepare_queries;

  $self;
}

sub _prepare_queries {
  my $self = shift;

  # Vendor name from slug
  $self->{vendor_name} = $self->{dbh}->prepare(
    'SELECT vendor_name FROM Vendors WHERE vendor_slug=?');

  # Product name from product_id
  $self->{product_name} = $self->{dbh}->prepare(
    'SELECT product_name FROM Products WHERE product_id=?');

  # Sum total of vendor's attestations by product
  $self->{vendor_totals_by_state} = $self->{dbh}->prepare(
    'SELECT product_name, COUNT(*) from Everything WHERE vendor_slug=? GROUP BY product_name');

  # Sum total of vendor's attestations by state
  $self->{vendor_totals_by_product} = $self->{dbh}->prepare(
    'SELECT state_name, COUNT(*) from Everything WHERE vendor_slug=? GROUP BY state_name');
}

sub DESTROY {
  my $self = shift;

  # Probably not necessary, but better safe than sorry
  $self->{dbh}->close if defined($self->{dbh});
}

# TODO: refactor w/AUTOLOAD, or factory and global symbol table manip
sub product_name {
  my ($self, $id) = @_;

  $self->{product_name}->execute($id);
  my $result_ref = $self->{product_name}->fetchrow_arrayref;
  return defined($result_ref) ? $result_ref->[0] : '';
}

sub vendor_name {
  my ($self, $slug) = @_;

  $self->{vendor_name}->execute($slug);
  my $result_ref = $self->{vendor_name}->fetchrow_arrayref;
  return defined($result_ref) ? $result_ref->[0] : '';
}

sub vendor_totals_by_state {
  my ($self, $vendor) = @_;

  $self->{vendor_totals_by_state}->execute($vendor);

  # Iterator
  return sub {
    $self->{vendor_totals_by_state}->fetchrow_arrayref;
  }
}

sub vendor_totals_by_product {
  my ($self, $vendor) = @_;

  $self->{vendor_totals_by_product}->execute($vendor);

  # Iterator
  return sub {
    $self->{vendor_totals_by_product}->fetchrow_arrayref;
  }
}

1;

# vi:sw=2 ts=2 sts=2 et:
