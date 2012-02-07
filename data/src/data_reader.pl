#!/usr/bin/perl -w
use strict;
use warnings;
#use Text::ParseWords;
use DBI;

sub delete_tables
{
    my $dbh = shift;
    $dbh->do ("DROP VIEW IF EXISTS Everything");
    $dbh->do ("DROP VIEW IF EXISTS Original");
    $dbh->do ("DROP VIEW IF EXISTS ProductVersions");
    $dbh->do ("DROP VIEW IF EXISTS VendorProducts");
    $dbh->do ("DROP TABLE IF EXISTS Attestations");
    $dbh->do ("DROP TABLE IF EXISTS ProviderSpecialties");
    $dbh->do ("DROP TABLE IF EXISTS Versions");
    $dbh->do ("DROP TABLE IF EXISTS Products");
    $dbh->do ("DROP TABLE IF EXISTS Vendors");
    $dbh->do ("DROP TABLE IF EXISTS StateCodes");
}

sub create_tables
{
    my $dbh = shift;
    
    $dbh->do ("CREATE TABLE StateCodes(
              state_code CHAR(2) PRIMARY KEY,
              state_name VARCHAR(40))");
    
    $dbh->do ("CREATE TABLE Vendors(
              vendor_id INTEGER PRIMARY KEY AUTO_INCREMENT,
              vendor_name VARCHAR(100))") or die "Cannot create table: " . $dbh->errstr ();
    $dbh->do ("ALTER TABLE Vendors
              ADD CONSTRAINT no_duplicate_vendors UNIQUE (vendor_name)");

    $dbh->do ("CREATE TABLE Products(
              product_id INTEGER PRIMARY KEY AUTO_INCREMENT,
              vendor_id INTEGER REFERENCES Vendors(vendor_id),
              product_name VARCHAR(100))") or die "Cannot create table: " . $dbh->errstr ();
    $dbh->do ("ALTER TABLE Products
              ADD CONSTRAINT no_duplicate_products UNIQUE (vendor_id,product_name)");

    $dbh->do ("CREATE TABLE Versions(
              version_id INTEGER PRIMARY KEY AUTO_INCREMENT,
              product_id INTEGER REFERENCES Products(product_id),
              version_name VARCHAR(100))") or die "Cannot create table: " . $dbh->errstr ();
    $dbh->do ("ALTER TABLE Versions
              ADD CONSTRAINT no_duplicate_versions UNIQUE (product_id,version_name)");

    $dbh->do ("CREATE TABLE ProviderSpecialties(
              provider_specialty_id INTEGER PRIMARY KEY AUTO_INCREMENT,
              provider_specialty_name VARCHAR(100))");

    $dbh->do ("CREATE TABLE Attestations(
              attestation_id INTEGER PRIMARY KEY AUTO_INCREMENT,
              version_id INTEGER REFERENCES Versions(version_id),
              provider_specialty_id INTEGER REFERENCES ProviderSpecialties(provider_specialty_id),
              attestation_classification CHAR(1),
              attestation_setting CHAR(1),
              provider_state CHAR(2) REFERENCES StateCodes(state_code),
              provider_type CHAR(1),
              attestation_month INTEGER,
              program_year INTEGER,
              payment_year INTEGER,
              program_type VARCHAR(80),
              attestation_gov_id VARCHAR(20))") or die "Cannot create table: " . $dbh->errstr ();
    
    $dbh->do ("CREATE VIEW VendorProducts AS
              SELECT Vendors.vendor_name , Products.product_name
              FROM Vendors INNER JOIN Products ON Vendors.vendor_id=Products.vendor_id") or die "Cannot create view: " . $dbh->errstr ();
    $dbh->do ("CREATE VIEW ProductVersions AS
              SELECT Vendors.vendor_id , Vendors.vendor_name ,
              Products.product_id , Products.product_name ,
              Versions.version_id , Versions.version_name
              FROM
              Versions
              INNER JOIN Products ON Versions.product_id=Products.product_id
              INNER JOIN Vendors ON Vendors.vendor_id = Products.vendor_id") or die "Cannot create view: " . $dbh->errstr ();
    
    $dbh->do ("CREATE VIEW Original AS
              SELECT Vendors.vendor_name,
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
    
    $dbh->do ("CREATE VIEW Everything AS
              SELECT
              Vendors.vendor_id,
              Products.product_id,
              Versions.version_id,
              Attestations.provider_specialty_id,
              Attestations.provider_state,
              Vendors.vendor_name,
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

sub read_state_codes
{
    my $dbh = shift;
    my $file = "../raw/states.csv";
    open(FILE,$file) or die "Couldn't open $file";
    
    my $add_state = $dbh->prepare("INSERT INTO StateCodes (state_code,state_name)
                                  VALUES (?,?)");
    
    while(my $line = <FILE>)
    {
        chomp($line);
        my @states = split(/,/,$line);
        $add_state->execute(@states);
    }
}

sub read_data
{
    my $dbh = shift;
    
    my $file = "../raw/ehr_data.txt";
    open(FILE,$file) or die "Couldn't open $file";

    my $get_vendor = $dbh->prepare("SELECT vendor_id FROM Vendors WHERE vendor_name=?") or die "Cannot prepare: " . $dbh->errstr ();
    my $add_vendor = $dbh->prepare("INSERT INTO Vendors (vendor_name) VALUES (?)");

    my $get_product = $dbh->prepare("SELECT product_id FROM Products WHERE vendor_id = ? AND product_name = ?") or die "Cannot prepare: " . $dbh->errstr ();
    my $add_product = $dbh->prepare("INSERT INTO Products (vendor_id , product_name) VALUES (?,?)");

    my $get_version = $dbh->prepare("SELECT version_id FROM Versions WHERE product_id = ? AND version_name = ?") or die "Cannot prepare: " . $dbh->errstr ();
    my $add_version = $dbh->prepare("INSERT INTO Versions (product_id , version_name) VALUES (?,?)");

    my $get_provider_specialty = $dbh->prepare("SELECT provider_specialty_id FROM ProviderSpecialties WHERE provider_specialty_name = ?") or die "Cannot prepare: " . $dbh->errstr ();
    my $add_provider_specialty = $dbh->prepare("INSERT INTO ProviderSpecialties (provider_specialty_name) VALUES (?)");
    
    my $find_state_code = $dbh->prepare("SELECT state_code FROM StateCodes WHERE state_name = ?");
    
    my $add_attestation = $dbh->prepare("INSERT INTO Attestations
                                        (version_id,
                                        provider_specialty_id,
                                        attestation_classification,
                                        attestation_setting,
                                        attestation_month,
                                        provider_state,
                                        provider_type,
                                        program_year,
                                        payment_year,
                                        program_type,
                                        attestation_gov_id)
                                        VALUES (?,?,?,?,?,?,?,?,?,?,?)");
    
    my $discard_headers = <FILE>;
    while (my $line = <FILE>)
    {   
        chomp($line);
    
        my @tokenized_line = split /\t/,$line;
             
        # Get vendor ID (adding vendor to Vendor table if necessary)
        my @handles = ($get_vendor,$add_vendor);
        my @args = (shift @tokenized_line);
        my $vendor_id = add_if_necessary(\@handles,\@args);
        
        # Get product ID (adding product to Product table if necessary)
        @handles = ($get_product,$add_product);
        @args = ($vendor_id,shift @tokenized_line);
        my $product_id = add_if_necessary(\@handles,\@args);
    
        # Get version ID (adding version to Version table if necessary)
        @handles = ($get_version,$add_version);
        @args = ($product_id,shift @tokenized_line);
        my $version_id = add_if_necessary(\@handles,\@args);
        
        # Get provider specialty ID (adding provider specialty to ProviderSpecialty table if necessary)
        @handles = ($get_provider_specialty,$add_provider_specialty);
        @args = (splice(@tokenized_line,5,1));
        my $provider_id = add_if_necessary(\@handles,\@args);
        
        # Add foreign keys to the beginning of our line
        unshift @tokenized_line,($version_id,$provider_id);
           
        # Shorten attestation_classification to one-character code
        die unless ($tokenized_line[2] eq "Complete EHR" or $tokenized_line[2] eq "Modular EHR");
        $tokenized_line[2] = substr $tokenized_line[2],0,1;
        
        # Shorten attestation_setting to one-character code
        die unless ($tokenized_line[3] eq "Ambulatory" or $tokenized_line[3] eq "Inpatient");
        $tokenized_line[3] = substr $tokenized_line[3],0,1;
        
        # Replace state with state code
        $find_state_code->execute($tokenized_line[5]);
        my @stn = $find_state_code->fetchrow_array();
        $tokenized_line[5] = $stn[0];
        
        # Shorten provider_type to one-character code
        die unless ($tokenized_line[6] eq "EP" or $tokenized_line[6] eq "Hospital");
        $tokenized_line[6] = substr $tokenized_line[6],0,1;
        
        # Shorten program_type to one-character code
        die unless ($tokenized_line[9] eq "Medicare" or $tokenized_line[9] eq "Medicare/Medicaid");
        $tokenized_line[9] = $tokenized_line[9] eq "Medicare" ? "M" : "B";
        
        # @tokenized_line now contains all arguments for adding row to attestations table
        $add_attestation->execute(@tokenized_line);
    }
}

sub add_if_necessary
{
    my $handles = shift;
    my $arguments = shift;
    
    $$handles[0]->execute(@$arguments);
    my @row = $$handles[0]->fetchrow_array;
    if(!@row)
    {
        $$handles[1]->execute(@$arguments);
        $$handles[0]->execute(@$arguments);
        @row = $$handles[0]->fetchrow_array;
    }
    $$handles[0]->finish;
    return $row[0];
}



#my $dbh = DBI->connect ("dbi:ODBC:Test") or die "Cannot connect: $DBI::errstr";
my $dbh = DBI->connect("dbi:mysql:sehrvy") or die "Cannot connect: $DBI::errstr";
#my $sth;

delete_tables($dbh);
create_tables($dbh);
read_state_codes($dbh);
read_data($dbh);
tree_output($dbh);
#query_test($dbh);
#count_products($dbh);
#print_specialties($dbh);
#print_attestations($dbh);
#print_original($dbh);

$dbh->disconnect;

sub count_products
{
    my $dbh = shift;
    my $sth = $dbh->prepare("SELECT vendor_id , COUNT(version_id) FROM ProductVersions GROUP BY vendor_id ORDER BY count(version_id) DESC");
    $sth->execute();
    my $results = $sth->fetchall_arrayref();
    $sth = $dbh->prepare("SELECT vendor_name FROM Vendors WHERE vendor_id = ?");
    foreach (@$results)
    {
        $sth->execute($$_[0]);
        my @name = $sth->fetchrow_array;
        print "$name[0] - $$_[1]\n";
    }
    $sth->finish;
}

sub print_attestations
{
    my $dbh = shift;
    my $zz = $dbh->prepare("SELECT * FROM Attestations");
    $zz->execute();
    while(my @qq = $zz->fetchrow_array)
    {
        print "@qq\n";
    }
}

sub print_specialties
{
    my $dbh = shift;
    my $zz = $dbh->prepare("SELECT * FROM ProviderSpecialties");
    $zz->execute();
    while(my @qq = $zz->fetchrow_array)
    {
        print "@qq\n";
    }
}



sub query_test
{
    my $dbh = shift;
    my $zz = $dbh->prepare("SELECT product_id FROM Products WHERE product_name LIKE '%at%'");
    $zz->execute();
    my $ans = $zz->fetchall_arrayref();

    $zz = $dbh->prepare("SELECT vendor_name , product_name , version_name FROM ProductVersions WHERE product_id = ?");
    foreach (@$ans)
    {
        $zz->execute(@$_[0]);
        while (my @qq = $zz->fetchrow_array)
        {
            print map{"$_;"} @qq;
            print "\n";
        }
        $zz->finish;
    }
}

sub print_original
{
    my $dbh = shift;
    my $zz = $dbh->prepare("SELECT * FROM Original");
    $zz->execute();
    while(my @qq = $zz->fetchrow_array)
    {
        $qq[3] = $qq[3] eq "C" ? "Complete EHR" : "Modular EHR";
        $qq[4] = $qq[4] eq "A" ? "Ambulatory" : "Inpatient";
        $qq[7] = $qq[7] eq "E" ? "EP" : "Hospital";
        $qq[11] = $qq[11] eq "M" ? "Medicare" : "Medicare/Medicaid";
        my $last = pop @qq;
        print map{"$_\t"} @qq;
        print "$last\n";
    }
}





sub tree_output
{
    my $dbh = shift;
    my $zz = $dbh->prepare("SELECT vendor_id , vendor_name FROM Vendors");
    $zz->execute();
    my $vendor_list = $zz->fetchall_arrayref();
    
    $zz = $dbh->prepare("SELECT product_id , product_name FROM Products WHERE vendor_id = ?");
    foreach my $temp (@$vendor_list)
    {
        $zz->execute($$temp[0]);
        $$temp[0]=$zz->fetchall_arrayref();
    }

    $zz = $dbh->prepare("SELECT version_name FROM Versions WHERE product_id = ?");
    foreach my $outer(@$vendor_list)
    {
        print "$$outer[1]\n";
        my @continue = $$outer[0];
        foreach my $inner(@continue)
        {
           foreach my $inner2(@$inner)
           {
                #print "$$inner2[0]\n";
                print "\t$$inner2[1]\n";
                $zz->execute($$inner2[0]);
                while (my @qq = $zz->fetchrow_array)
                {
                    print "\t\t@qq\n";
                }
           }
        }
    }
}