use strict;
use warnings;

package Sehrvy::Server;

use base qw(Net::Server::HTTP);

sub process_http_request {
	my $self = shift;

	print "Content-type: text/html\n\n";
	print "<form method=post action=/bam><input type=text name=foo><input type=submit></form>\n";

	if (require Data::Dumper) {
		local $Data::Dumper::Sortkeys = 1;
		my $form = {};
		if (require CGI) {  my $q = CGI->new; $form->{$_} = $q->param($_) for $q->param;  }
		print "<pre>".Data::Dumper->Dump([\%ENV, $form], ['*ENV', 'form'])."</pre>";
	}
}

1;
