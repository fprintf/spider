#!/usr/bin/perl
#

use strict;
use warnings;

use threads;
use threads::shared;
use LWP::UserAgent;

# 1. Remove the first website from the queue (FIFO i think makes sense here)
# 2. Get all links from website and add them to the queue (if they're not already in the queue)
# 3. Query known common directories/files from website (robots.txt check etc..) and add
# those paths as links to the queue as well if they hold any additional crawling information
# 5. Index the url based on the text content on the page in some distributed database (elastic search? postgresql? mongodb?)
# 4. Repeat from step 1
#
# Expanding on step 2: we need to ensure we're not adding items to the queue that already exist
# in the queue because that is a waste of time/duplicate work. To do this we need to keep track of all the known
# URLs we have and quickly check if we've indexed them already or they're already in queue to be indexed.
#
# If they already exist in the index db then we should check our expire time and see if we should update their 
# content, if not we're done and don't add this to the queue.
#
# If the URL already exists in the queue to be indexed then we are again done because it's already been queued. Checking this
# may be difficult because we need a quick way to check if the item is in the queue, doing a linear search on the queue for a distributed system with potentially millions of items is not a good idea. Binary search is good enough though or O(log n) with a tree 
#
#
#
#

{
    package Spider::Queue;
    my $expire_time = 300; # Time before we re-check a url
    sub new {
        my $class = shift;
        my $self = bless {}, $class;
        my @jobqueue :shared;
        my %db :shared;
        $self->{queue} = \@jobqueue;
        $self->{db} = \%db;
        return $self;
    }

    # Add an item to the job queue
    sub add {
        my ($self, @jobs) = @_;
        my @filtered = grep { 
            if (!exists($self->{db}{$_}) || $self->{db}{$_}{expires} > time) {
                lock($self->{db});
                my %hash :shared = (expires => time + $expire_time);
                $self->{db}{$_} = \%hash;
                return 1;
            }
            return 0;
        } @jobs;
        lock($self->{queue});
        push(@{$self->{queue}}, @filtered);
    }

    # Get an item from the queue
    sub shift {
        my ($self) = @_;

        lock($self->{queue});
        my $item = shift @{$self->{queue}};
        return $item;
    }

    # Return true if the queue is empty
    sub empty {
        my ($self) = @_;
        return !@{$self->{queue}};
    }

    1;
}


my @targets = (
    'http://nytimes.com/'
);

my $job_queue = Spider::Queue->new;


foreach my $job (@targets) {
    $job_queue->add($job);
}

my @workers = make_workers(sub {
        my ($url) = @_;
        my $ua = LWP::UserAgent->new();
        my @new_links = search_url($ua, $url);

        foreach my $link (@new_links) {
            $job_queue->add($link);
        }
    }
);

cleanup_workers(@workers);


# Array of worker threadsb
sub make_workers {
    my ($function, $count) = @_;
    $count ||= 4;

    # Our list of threads we will be returning
    my @workers;

    for (my $i = 0; $i < $count; ++$i) {
        my $thread = threads->create(
            sub { 
                my $count = 10;
                while (1) {
                    if ($job_queue->empty()) {
                        sleep(1);
                        # Only try $count times again before exiting
                        if (--$count == 0) {
                            last;
                        }
                        next;
                    }

                    my $job = $job_queue->shift();
                    $function->($job);
                }
            }
        );

        push(@workers, $thread);
    }

    return @workers;
}

sub cleanup_workers {
    my (@workers) = @_;

    while (my $worker = shift @workers) {
        # Join to thread and let it finish
        $worker->join();
    }
}

sub fixup_link {
    my ($link, $baseurl) = @_;
    if ($link =~ /^(?:(https?):\/\/|www\.)(.*)$/) {
        my $proto = $1 || 'http';
        $link = sprintf("%s://%s", $proto, $2);
    } else {
        $link =~ s/\/*$//g;
        $link = sprintf("%s/%s", $baseurl, $link);
    }
    return $link;
}

# Find all the links in content and return an array of them
sub get_links {
    my ($content, $baseurl) = @_;
    my @links;
    my $exempt_re = qr/\.(?:png|jpe?g|gif|mov|mp4|avi|vgif|webm)$/;


    while ($content =~ /[hH][rR][Ee][Ff]\s*=\s*['"]?\s*([^'"\s]+)\s*['"]?/gms) {
        my $link = fixup_link($1, $baseurl);
        if ($link =~ /$exempt_re/i) { 
            print STDERR "exempted: [$link]\n";
            next;
        }
        print STDERR "found link: [$link]\n";
        push(@links, $link);
    }

    return @links;
}


# Get url and return all the links as an array on the page 
# TODO also index the content of the page for fulltext search
sub search_url {
    my ($ua, $url) = @_;
    my @links;

    sleep 5 * rand(10);
    print STDERR "fetching url: $url\n";
    my $r = $ua->get($url);
    if (!$r->is_success()) {
        print STDERR "failed to fetch: $url: ".$r->status_line()."\n";
        return ();
    }

    # TODO index content for fulltext search and index by url
    #

    # Parse out all the links in the page
    @links = get_links($r->decoded_content(), $url);
    
    return @links;
}


1;
