use strict;
use warnings;

use DB;

use File::Spec;
use JSON;
use HTML::Template;
use 5.008;

package Sehrvy::Server;
use base qw(Net::Server::HTTP);

# Global Package Variables
our @valid_methods = ('GET', 'POST');
our $ROOT_DIR = (File::Spec->splitpath(__FILE__))[1];

our $db = DB->new;


# Request Handling

sub process_http_request {
  my $self = shift;

  $self->dispatch($ENV{REQUEST_METHOD}, $ENV{PATH_INFO}, $ENV{QUERY_STRING});
}

sub dispatch {
  my ($self, $request_method, $path_info, $query_string) = @_;

  # Only support valid accesses
  if (grep {$_ eq $request_method} @valid_methods) {

    for ($path_info) {
      if(m{^/js}) { $self->serve('content' . $path_info, 'text/javascript')}
      elsif(m{^/map}) { $self->serve('content/template/map.tmpl', undef, {name => 'Mappy'})}
      elsif(m{^/vendor/(.*)}) { $self->serve('content/template/vendor.tmpl', undef, {name => $1})}
      elsif(m{^/test}) { $self->test_form }
      elsif(m{^/query}) { $self->test_query }
      elsif(m{^/$}) { $self->serve('content/static/index.html')}
      else { $self->err_unknown_path($path_info) }
    }
  } else {
    $self->err_unimplemented_method($request_method);
  }
}

sub serve {
  my ($self, $file, $type, $templ_params) = @_;

  content_type($type);

  my $full_path = File::Spec->catfile($ROOT_DIR, $file);

  # Templates get generated, regular files get served straight up

  # Templates are defined by the extension .tmpl
  if ($file =~ /\.tmpl$/) {

    # Load the template
    my $templ = HTML::Template->new(
      filename => $full_path,
      cache    => 1,
      utf8     => 1
    );

    # Apply the supplied parameters
    $templ->param($templ_params);

    # Print to stdout (i.e. the client)
    print $templ->output;

  } else {
    open my $fh, '<', $full_path or return $self->err_unknown_path($file);

    while (defined(my $line = <$fh>)) {
      print $line;
    }
  }
}


# Content Delivery Utilities

sub content_type {
  my $type = shift || 'text/html';

  print "Content-type: $type\n\n";
}


# Errors

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


# Testing functions

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
  #print JSON::to_json([2, 3, 4, {"asdf" => "boo", "jasks" => "narf"}, ["3", 11, 14, 12.3, "doggy"]]);
  print JSON::to_json({
    cols => [
      {label => 'State', type => 'string'}, 
      {label => 'Score', type => 'number'} 
    ],
    rows => [
      {c => [{v => "US-TX"}, {v => 150}]},
      {c => [{v => "US-MA"}, {v => 250}]},
      {c => [{v => "US-MN"}, {v => 350}]},
      {c => [{v => "US-WI"}, {v => 450}]}
    ]
  });
}

1;

# vi:sw=2 ts=2 sts=2 et:
