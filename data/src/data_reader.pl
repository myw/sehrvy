#!/usr/bin/perl -w
use strict;
use warnings;
use DBI;
use 5.008;


MAIN: {
  my $dbh = DBI->connect('dbi:mysql:sehrvy','sehrvy_admin') or die "Cannot connect: $DBI::errstr";

  delete_tables($dbh);
  create_tables($dbh);
  read_state_codes($dbh);
  read_data($dbh);

  $dbh->disconnect;
}

# Delete all tables in the database
sub delete_tables {
  my $dbh = shift;
  $dbh->do("DROP VIEW IF EXISTS Everything");
  $dbh->do("DROP VIEW IF EXISTS Original");
  $dbh->do("DROP VIEW IF EXISTS ProductVersions");
  $dbh->do("DROP VIEW IF EXISTS VendorProducts");
  $dbh->do("DROP TABLE IF EXISTS Attestations");
  $dbh->do("DROP TABLE IF EXISTS ProviderSpecialties");
  $dbh->do("DROP TABLE IF EXISTS Versions");
  $dbh->do("DROP TABLE IF EXISTS Products");
  $dbh->do("DROP TABLE IF EXISTS Vendors");
  $dbh->do("DROP TABLE IF EXISTS StateCodes");
}


# Create all tables
sub create_tables {
  my $dbh = shift;

  $dbh->do("CREATE TABLE StateCodes(
           state_code CHAR(2) PRIMARY KEY,
           state_name VARCHAR(40) NOT NULL)");

  $dbh->do("CREATE TABLE Vendors(
           vendor_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
           vendor_slug VARCHAR(100) NOT NULL,
           vendor_name VARCHAR(100) NOT NULL,
           UNIQUE KEY no_duplicate_vendors (vendor_name),
           UNIQUE KEY no_duplicate_vendor_slugs (vendor_slug)
           )") or die "Cannot create table: " . $dbh->errstr ();

  $dbh->do("CREATE TABLE Products(
           product_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
           vendor_id INT UNSIGNED NOT NULL,
           product_name VARCHAR(100) NOT NULL,
           FOREIGN KEY(vendor_id) REFERENCES Vendors(vendor_id),
           UNIQUE KEY no_duplicate_products (vendor_id,product_name)
           )") or die "Cannot create table: " . $dbh->errstr ();

  $dbh->do("CREATE TABLE Versions(
           version_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
           product_id INT UNSIGNED NOT NULL,
           version_name VARCHAR(100) NOT NULL,
           FOREIGN KEY(product_id) REFERENCES Products(product_id),
           UNIQUE KEY no_duplicate_versions (product_id,version_name)
           )") or die "Cannot create table: " . $dbh->errstr ();

  $dbh->do("CREATE TABLE ProviderSpecialties(
           provider_specialty_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
           provider_specialty_name VARCHAR(100) NOT NULL)");

  $dbh->do("CREATE TABLE Attestations(
           attestation_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
           version_id INT UNSIGNED NOT NULL,
           provider_specialty_id INT UNSIGNED NOT NULL,
           attestation_classification CHAR(1) NOT NULL,
           attestation_setting CHAR(1) NOT NULL,
           provider_state CHAR(2) NOT NULL,
           provider_type CHAR(1) NOT NULL,
           attestation_month SMALLINT UNSIGNED NOT NULL,
           attestation_year INTEGER NOT NULL,
           program_year INT UNSIGNED,
           payment_year INT UNSIGNED,
           program_type CHAR(1) NOT NULL,
           attestation_gov_id INT UNSIGNED NOT NULL,
           FOREIGN KEY (version_id) REFERENCES Versions(version_id),
           FOREIGN KEY (provider_specialty_id) REFERENCES ProviderSpecialties(provider_specialty_id),
           FOREIGN KEY (provider_state) REFERENCES StateCodes(state_code)
           )") or die "Cannot create table: " . $dbh->errstr ();

  $dbh->do("CREATE VIEW VendorProducts AS
           SELECT Vendors.vendor_name , Vendors.vendor_slug , Products.product_name
           FROM Vendors INNER JOIN Products ON Vendors.vendor_id=Products.vendor_id") or die "Cannot create view: " . $dbh->errstr ();
  $dbh->do("CREATE VIEW ProductVersions AS
           SELECT Vendors.vendor_id , Vendors.vendor_name , Vendors.vendor_slug , 
           Products.product_id , Products.product_name ,
           Versions.version_id , Versions.version_name
           FROM
           Versions
           INNER JOIN Products ON Versions.product_id=Products.product_id
           INNER JOIN Vendors ON Vendors.vendor_id = Products.vendor_id") or die "Cannot create view: " . $dbh->errstr ();

  $dbh->do("CREATE VIEW Original AS
           SELECT
           Vendors.vendor_name,
           Products.product_name,
           Versions.version_name,
           Attestations.attestation_classification,
           Attestations.attestation_setting,
           Attestations.attestation_month,
           Attestations.attestation_year,
           StateCodes.state_name,
           Attestations.provider_type,
           ProviderSpecialties.provider_specialty_name,
           Attestations.program_year,
           Attestations.payment_year,
           Attestations.program_type,
           Attestations.attestation_gov_id,
           Attestations.attestation_id
           FROM
           Attestations
           INNER JOIN Versions on Versions.version_id=Attestations.version_id
           INNER JOIN Products ON Versions.product_id=Products.product_id
           INNER JOIN Vendors ON Vendors.vendor_id=Products.vendor_id
           INNER JOIN ProviderSpecialties ON Attestations.provider_specialty_id = ProviderSpecialties.provider_specialty_id
           INNER JOIN StateCodes ON Attestations.provider_state = StateCodes.state_code");

  $dbh->do("CREATE VIEW Everything AS
           SELECT
           Vendors.vendor_id,
           Products.product_id,
           Versions.version_id,
           Attestations.provider_specialty_id,
           Attestations.provider_state,
           Vendors.vendor_name,
           Vendors.vendor_slug,
           Products.product_name,
           Versions.version_name,
           Attestations.attestation_classification,
           Attestations.attestation_setting,
           Attestations.attestation_month,
           StateCodes.state_name,
           Attestations.provider_type,
           ProviderSpecialties.provider_specialty_name,
           Attestations.program_year,
           Attestations.payment_year,
           Attestations.program_type,
           Attestations.attestation_gov_id
           FROM
           Attestations
           INNER JOIN Versions on Versions.version_id=Attestations.version_id
           INNER JOIN Products ON Versions.product_id=Products.product_id
           INNER JOIN Vendors ON Vendors.vendor_id=Products.vendor_id
           INNER JOIN ProviderSpecialties ON Attestations.provider_specialty_id = ProviderSpecialties.provider_specialty_id
           INNER JOIN StateCodes ON Attestations.provider_state = StateCodes.state_code");
}


# Read state codes and names into the database
sub read_state_codes {
  my $dbh = shift;
  my $file = "../raw/states.csv";
  open(FILE,$file) or die "Couldn't open $file";
  our %cached_states;

  my $add_state = $dbh->prepare("INSERT INTO StateCodes (state_code,state_name)
                                VALUES (?,?)");

  while(my $line = <FILE>) {
    chomp($line);
    my @states = split(/,/,$line);
    $add_state->execute(@states);
    $cached_states{lc($states[1])} = $states[0];
  }
}

# Read EHR data into the database
sub read_data {

  my $dbh = shift;
  my %cached_vendors;
  my %used_vendor_slugs;
  my %cached_vendor_names; # Use only the names to cache slug calculation
  my %cached_products;
  my %cached_versions;
  my %cached_specialties;
  our %cached_states;

  my $file = "../raw/ehr_data_sanitized.txt";
  open(FILE,$file) or die "Couldn't open $file";

  my $get_vendor = $dbh->prepare("SELECT vendor_id FROM Vendors WHERE vendor_slug =? AND vendor_name = ?") or die "Cannot prepare: " . $dbh->errstr ();
  my $add_vendor = $dbh->prepare("INSERT INTO Vendors (vendor_slug, vendor_name) VALUES (?,?)");

  my $get_product = $dbh->prepare("SELECT product_id FROM Products WHERE vendor_id = ? AND product_name = ?") or die "Cannot prepare: " . $dbh->errstr ();
  my $add_product = $dbh->prepare("INSERT INTO Products (vendor_id, product_name) VALUES (?,?)");

  my $get_version = $dbh->prepare("SELECT version_id FROM Versions WHERE product_id = ? AND version_name = ?") or die "Cannot prepare: " . $dbh->errstr ();
  my $add_version = $dbh->prepare("INSERT INTO Versions (product_id, version_name) VALUES (?,?)");

  my $get_provider_specialty = $dbh->prepare("SELECT provider_specialty_id FROM ProviderSpecialties WHERE provider_specialty_name = ?") or die "Cannot prepare: " . $dbh->errstr ();
  my $add_provider_specialty = $dbh->prepare("INSERT INTO ProviderSpecialties (provider_specialty_name) VALUES (?)");

  my $discard_headers = <FILE>;

  my @lines;
  while (my $line = <FILE>) {
    chomp($line);

    my @tokenized_line = split /\t/,$line;

    # Get vendor ID (adding vendor to Vendor table if necessary)
    my @handles = ($get_vendor,$add_vendor);
    my @args = (shift @tokenized_line);
    unshift @args, slugify($args[0], \%cached_vendor_names, \%used_vendor_slugs); # slugify vendor name and put it in front
    my $vendor_id = add_if_necessary(\@handles,\@args,\%cached_vendors);

    # Get product ID (adding product to Product table if necessary)
    @handles = ($get_product,$add_product);
    @args = ($vendor_id,shift @tokenized_line);
    my $product_id = add_if_necessary(\@handles,\@args,\%cached_products);

    # Get version ID (adding version to Version table if necessary)
    @handles = ($get_version,$add_version);
    @args = ($product_id,shift @tokenized_line);
    my $version_id = add_if_necessary(\@handles,\@args,\%cached_versions);

    # Get provider specialty ID (adding provider specialty to ProviderSpecialty table if necessary)
    @handles = ($get_provider_specialty,$add_provider_specialty);
    @args = (splice(@tokenized_line,6,1));
    my $provider_id = add_if_necessary(\@handles,\@args,\%cached_specialties);

    # Add foreign keys to the beginning of our line
    unshift @tokenized_line,($version_id,$provider_id);

    # Shorten attestation_classification to one-character code
    die unless ($tokenized_line[2] eq "Complete EHR" or $tokenized_line[2] eq "Modular EHR");
    $tokenized_line[2] = substr $tokenized_line[2],0,1;

    # Shorten attestation_setting to one-character code
    die unless ($tokenized_line[3] eq "Ambulatory" or $tokenized_line[3] eq "Inpatient");
    $tokenized_line[3] = substr $tokenized_line[3],0,1;

    # Replace state with state code
    print "$tokenized_line[6]\n" unless exists($cached_states{lc($tokenized_line[6])});
    $tokenized_line[6] = $cached_states{lc($tokenized_line[6])};

    # Shorten provider_type to one-character code
    die unless ($tokenized_line[7] eq "EP" or $tokenized_line[7] eq "Hospital");
    $tokenized_line[7] = substr $tokenized_line[7],0,1;

    $tokenized_line[8] = undef if $tokenized_line[8] eq '.';
    $tokenized_line[9] = undef if $tokenized_line[9] eq '.';

    # Shorten program_type to one-character code
    die unless ($tokenized_line[10] eq "Medicare" or $tokenized_line[10] eq "Medicare/Medicaid");
    $tokenized_line[10] = $tokenized_line[10] eq "Medicare" ? "M" : "B";

    # @tokenized_line now contains all arguments for adding row to attestations table

    cached_add($dbh, \@tokenized_line,\@lines);
  }

  add_all($dbh, \@lines);
}

sub cached_add {
  my $dbh = shift;

  my $tokenized_line = shift;
  my $current = shift;
  push @$current,$tokenized_line;
  my $numel = @$current;

  add_all($dbh, $current) if ($numel >= 10000);
}

sub add_all {
  my $dbh = shift;

  my $add_attestation = $dbh->prepare_cached(
    "INSERT INTO Attestations
       (version_id,
        provider_specialty_id,
        attestation_classification,
        attestation_setting,
        attestation_month,
        attestation_year,
        provider_state,
        provider_type,
        program_year,
        payment_year,
        program_type,
        attestation_gov_id)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");

  my $current = shift;
  $dbh->{AutoCommit} = 0;

  while(@$current) {
    my $temp = shift @$current;
    $add_attestation->execute(@$temp);
  }

  $dbh->commit;
  $dbh->{AutoCommit} = 1;
}

sub add_if_necessary {

    my $handles = shift;
    my $arguments = shift;
    my $cached = shift;

    return $$cached{lc("@$arguments")} if exists $$cached{lc("@$arguments")};

    $$handles[1]->execute(@$arguments);
    $$handles[0]->execute(@$arguments);
    my @row = $$handles[0]->fetchrow_array;
    $$cached{lc("@$arguments")} = $row[0];

    return $row[0];
}

sub slugify {
  my ($string, $cache, $used_slugs) = @_;
  # track both previously calculated slugifications (cache), organized
  # by string, and previously used slugs, organized by slug

  return $cache->{$string} if $cache->{$string};

  my $new_string = lc $string;

  # Remove parentheticals
  $new_string =~ s/\(.*\)//g;

  # Remove non-hyphen dashes
  $new_string =~ s/\s+-\s+/ /g;

  # Turn anything that we can't easily parse into a single space
  $new_string =~ s/[^a-z0-9-]/ /g;
  $new_string =~ s/\s+/ /g;

  # Trim edge space
  $new_string =~ s/^\s+|\s+$//;

  # Keep pushing words onto sluglist until we have a unique slug
  my @words = split /\s/, $new_string;
  local $" = '-'; # ('foo', 'bar') -> 'foo-bar'
  my @sluglist;
  my $slug;

  do {
    # Add on the next word
    push(@sluglist, shift @words);

    $slug = "@sluglist";

  } while(exists($used_slugs->{$slug}));

  # Update the cache and used slugs hash
  $used_slugs->{$slug} = 1;
  $cache->{$string} = $slug;

  return $slug;
}

# vi:sw=2 ts=2 sts=2 et:
