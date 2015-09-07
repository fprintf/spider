#!/usr/bin/perl
#

use strict;
use warnings;

use threads;
use Thread::Queue;
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

my $ua = LWP::UserAgent->new();

my @targets = [
    'http://nytimes.com/'
];

my $job_queue = Thread::Queue->new();
# Array of worker threadsb
sub make_workers {
    my ($function, $count) = @_;
    $count ||= 4;

    # Our list of threads we will be returning
    my @workers;

    for (my $i = 0; $i < $count; ++$i) {
        my $thread = threads->create(
            sub { 
                while (my $job = $job_queue->dequeue()) {
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

    # Jobs left in queue represent work that won't get done until
    # we start making this distributed which those will go into the global queue
    # then
    my $remaining = $job_queue->pending();
    if ($remaining > 0) {
        print STDERR "warning: $remaining items left uncomplete in job queue!\n";
    }
    # Close the queue for now so the workers know
    # to stop waiting for new jobs
    $job_queue->end();

    while (my $worker = shift @workers) {
        # Join to thread and let it finish
        $worker->join();
    }
}

sub add_job {
    my ($job) = @_;
    $job_queue->enqueue($job);
}


# Get url and return all the links as an arrayref on the page 
# TODO also index the content of the page for fulltext search
sub search_url {
    my ($ua, $url) = @_;
    my @links;

    my $r = $ua->get($url);
    if (!$r->is_success()) {
        print STDERR "failed to fetch: $url: ".$r->status_line()."\n";
        return ();
    }

    # TODO index content for fulltext search and index by url
    #
    #
    
    return @links;
}


1;
