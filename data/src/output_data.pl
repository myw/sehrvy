#!/usr/bin/perl -w
use strict;
use warnings;
use DBI;

my $dbh = DBI->connect("dbi:mysql:sehrvy") or die "Cannot connect: $DBI::errstr";

#count_products($dbh);
#print_attestations($dbh);
#print_specialties($dbh);
#query_test($dbh);
#print_original($dbh);
tree_output($dbh);

$dbh->disconnect;


# Output a list of vendors and the number of different product versions
# associated with each vendor
sub count_products
{
    my $dbh = shift;
    my $sth = $dbh->prepare("SELECT vendor_name , COUNT(version_id)
                            FROM ProductVersions
                            GROUP BY vendor_id
                            ORDER BY count(version_id) DESC");
    $sth->execute();
    
    while (my @row = $sth->fetchrow_array())
    {
        print "$row[0] - $row[1]\n";
    }
    $sth->finish;
}


# Print the contents of the attestations table
sub print_attestations
{
    my $dbh = shift;
    my $sth = $dbh->prepare("SELECT * FROM Attestations");
    $sth->execute();
    while(my @row = $sth->fetchrow_array)
    {
        print "@row\n";
    }
}

# Print a list of all specialties in the database
sub print_specialties
{
    my $dbh = shift;
    my $sth = $dbh->prepare("SELECT * FROM ProviderSpecialties");
    $sth->execute();
    while(my @row = $sth->fetchrow_array)
    {
        print "@row\n";
    }
}


# Print all products that match a certain text search condition
sub query_test
{
    my $dbh = shift;
    my $sth = $dbh->prepare("SELECT product_id FROM Products WHERE product_name LIKE '%at%'");
    $sth->execute();
    my $results = $sth->fetchall_arrayref();

    $sth = $dbh->prepare("SELECT vendor_name , product_name , version_name FROM ProductVersions WHERE product_id = ?");
    foreach (@$results)
    {
        $sth->execute(@$_[0]);
        while (my @row = $sth->fetchrow_array)
        {
            print map{"$_;"} @row;
            print "\n";
        }
        $sth->finish;
    }
}

# Output the data in a similar format to the input data
sub print_original
{
    my $dbh = shift;
    my $sth = $dbh->prepare("SELECT * FROM Original");
    $sth->execute();
    while(my @row = $sth->fetchrow_array)
    {
        $row[3] = $row[3] eq "C" ? "Complete EHR" : "Modular EHR";
        $row[4] = $row[4] eq "A" ? "Ambulatory" : "Inpatient";
        $row[7] = $row[7] eq "E" ? "EP" : "Hospital";
        $row[11] = $row[11] eq "M" ? "Medicare" : "Medicare/Medicaid";
        my $last = pop @row;
        print map{"$_\t"} @row;
        print "$last\n";
    }
}

# Output a hierarchical view of all vendors, products, and version
sub tree_output
{
    my $dbh = shift;
    my $sth = $dbh->prepare("SELECT vendor_id , vendor_name FROM Vendors");
    $sth->execute();
    my $vendor_list = $sth->fetchall_arrayref();
    
    $sth = $dbh->prepare("SELECT product_id , product_name FROM Products WHERE vendor_id = ?");
    foreach my $temp (@$vendor_list)
    {
        $sth->execute($$temp[0]);
        $$temp[0]=$sth->fetchall_arrayref();
    }

    $sth = $dbh->prepare("SELECT version_name FROM Versions WHERE product_id = ?");
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
                $sth->execute($$inner2[0]);
                while (my @qq = $sth->fetchrow_array)
                {
                    print "\t\t@qq\n";
                }
           }
        }
    }
}