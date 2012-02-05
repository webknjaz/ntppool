package NP::RRD::Worker;
use strict;
use warnings;
use File::Path qw(mkpath);
use File::Basename qw(dirname);
use LWP::Simple qw(get);
use JSON;
use Data::Dump 'pp';

my $pool_server = $ENV{POOL_SERVER} or die "ENV{POOL_SERVER} not set";

my $rrd_path = "$ENV{CBROOTLOCAL}/rrd/server";
mkpath "$rrd_path/graph/" unless -e "$rrd_path/graph";

sub rrd_path {
    my $server_id  = shift;
    my $monitor_id = shift;
    my $dir  = int( $server_id / 100 ) * 100;
    return "$rrd_path/$dir/" . $server_id . ($monitor_id ? "-$monitor_id" : "") . ".rrd";
}

sub graph_path {
    my ($server_id, $name) = @_;
    my $dir  = int( $server_id / 500 ) * 500;
    my $file = $dir . '/' . $server_id . ($name ? "-$name" : "") . ".png";
    return "$rrd_path/graph/" . $file;
}

my $map;
my $map_last_update;

sub update_map {
   return if $map and $map_last_update + 600 > time;
   eval {
       my $data = get( "http://${pool_server}/monitor/map?api_key=cr834nhcrah667cbmfk8s" ) or die "could not fetch map";
       my $new_map = decode_json($data) or die "json decode error";
       $map = $new_map;
       $map_last_update = time;
   };
   warn "Map update error: $@" if $@;
}

sub _check_ip {
    my $ip = shift or return 0;
    update_map;
    print pp($map);
    unless ($map->{$ip}) {
        warn "No server $ip in the map";
        return 0;
    }
    if ($map->{$ip}->{deleted}) {
        warn "Server $ip is deleted";
        return 0;
    }
    return $map->{$ip}->{id};;
}

sub update_rrds {
    my $job = shift;
    my $server_ip = ref $job ? $job->arg : $job;

    my $id = _check_ip($server_ip) or return 0;

    warn "ID: $id";

    my $path = rrd_path( $id, 0 );

    warn "path: $path";

    if (!-e $path) {
        warn "$path doesn't exist\n";
        create_rrd( $id, 0 );
    }

    my $since = RRDs::last $path;

    warn "SINC: $since";

    # fetch data

    # sort by monitor, highest # first

    # for each monitor
    #   update_rrd_data( $id, $monitor_id, \@data );

}

sub update_graph {
    my $job = shift;
    my ($server_ip, $monitor_id) = @{ $job->arg };
    my $id = _check_ip($server_ip) or return 0;


}

sub create_rrd {
    my ($server_id, $monitor_id) = @_;

    my $path = rrd_path( $server_id, $monitor_id );
    return if -e $path;

    my $dir = dirname($path);
    mkpath $dir, unless -d $dir;

    my $step = $monitor_id ? "20m" : "5m";

    my @ds = (
                 "--start", "1297152600",  # September 2008, oldest production archive date
                 "--step", $step,
                 "DS:score:GAUGE:3600:-100:20",   # heartbeat of ~2 hours, min value = -100, max = 20
                 "DS:offset:GAUGE:3600:-86400:86400",
                 "DS:step:GAUGE:3600:-10:5",
                 "RRA:AVERAGE:0.3:1:4320",   # 5/20 minutes, 15/60 days
                 "RRA:AVERAGE:0.3:3:3456",   # 15/60 minutes, 36/144 days
                 "RRA:AVERAGE:0.3:12:2304",  # 1/4 hours, 96/384 days
                 "RRA:AVERAGE:0.3:72:1825",  # 1 day, ~5 years
                 "RRA:MIN:0.3:3:3456",
                 "RRA:MIN:0.3:72:2048",
                 "RRA:MAX:0.3:3:3456",,
                 "RRA:MAX:0.3:72:2048",
                 "RRA:LAST:0.3:3:3456",,
                 "RRA:LAST:0.3:72:2048",
                );

    RRDs::create("$path", @ds);
    my $ERROR = RRDs::error();
    if ($ERROR) {
        die "$0: unable to create '$path': $ERROR\n";
    }

}


sub update_rrd_data {
    my ($server_id, $monitor_id, $data) = @_;

    #warn join " / ", $self->id, $self->ts; #, $self->ts->epoch;

    my $path = rrd_path($server_id, $monitor_id);

    RRDs::update $path,
        (
         '--template' => 'score:step:offset',
         map { join(":", $_->{ts},
                    $_->{score},
                    $_->{step},
                    (defined $_->{offset} ? $_->{offset} : 'U')
                   )
           } @$data
        );

    if (my $ERROR = RRDs::error()) {
        warn "$0: unable to update ",$path,": $ERROR\n";
    }

}



1;
