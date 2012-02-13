# The main server

use strict;
use warnings;

use Switch;
use JSON;

package Sehrvy::Server;
use base qw(Net::Server::HTTP);

our @valid_methods = ('GET', 'POST');

sub process_http_request {
	my $self = shift;

	$self->dispatch($ENV{REQUEST_METHOD}, $ENV{PATH_INFO}, $ENV{QUERY_STRING});
}

sub dispatch {
	my ($self, $request_method, $path_info, $query_string) = @_;

	# Only support valid accesses
	if (grep {$_ eq $request_method} @valid_methods) {

		switch ($path_info) {
			case m{^/test} { $self->test_form }
			case m{^/query} { $self->test_query }

			else { $self->err_unknown_path($path_info) }
		}
	} else {
		$self->err_unimplemented_method($request_method);
	}
}

sub content_type {
	my $type = shift || 'text/html';

	print "Content-type: $type\n\n";
}

sub test_form {
	my $self = shift;

	content_type;
	print "<html><head></head><body>\n";
	print "<form method=post action=/test/bam><input type=text name=foo><input type=submit></form>\n";

	if (require Data::Dumper) {
		local $Data::Dumper::Sortkeys = 1;
		my $form = {};
		if (require CGI) {  my $q = CGI->new; $form->{$_} = $q->param($_) for $q->param;  }
		print "<pre>".Data::Dumper->Dump([\%ENV, $form], ['*ENV', 'form'])."</pre>";
	}

	print "</body></html>\n";
}

sub test_query {
	my $self = shift;

	content_type('application/json');
	#print '[2, 3, 4, {"asdf": "boo", "jasks": "narf"}, ["3", 11, 14, 12.3, "doggy"]]';
	print JSON::to_json([2, 3, 4, {"asdf" => "boo", "jasks" => "narf"}, ["3", 11, 14, 12.3, "doggy"]]);

}

sub err_unknown_path {
	my $self = shift;
	my $path = shift;

	$self->send_status(404, 'Not Found');
	content_type;
	print "<h1>404 - Not Found</h1>\n";
	print "<p>The path <code>$path</code> is unavailable.</p>\n";
}

sub err_unimplemented_method {
	my $self = shift;
	my $request_method = shift;

	$self->send_501("Sehrvy does not support $request_method requests");
}
1;
