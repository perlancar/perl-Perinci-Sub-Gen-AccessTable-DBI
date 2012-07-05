#!perl

# test spec generation and the generated spec

use 5.010;
use strict;
use warnings;
use FindBin '$Bin';
use lib $Bin, "$Bin/t";

use Test::More 0.96;
require "testlib.pl";

my ($table_data, $table_spec) = gen_test_data();

test_gen(
    name => 'pk must be in fields',
    table_data => [],
    table_spec => {
        fields => {
            a => {schema=>'int*', index=>0, },
        },
        pk => 'b',
    },
    status => 400,
);

test_gen(
    name => 'pk must exist in table_spec',
    table_data => [],
    table_spec => {
        fields => {
            a => {schema=>'int*', index=>0, },
        },
    },
    status => 400,
);

test_gen(
    name => 'fields must exist in table_spec',
    table_data => [],
    table_spec => {
    },
    status => 400,
);

test_gen(
    name => 'fields in sort must exist in fields',
    table_data => [],
    table_spec => {
        fields => {
            a => {schema=>'int*', index=>0, },
        },
    },
    status => 400,
);

test_gen(
    name => 'spec generation',
    table_data => [],
    table_spec => $table_spec,
    status => 200,
    post_test => sub {
        my ($res) = @_;
        my $func = $res->[2]{code};
        my $meta = $res->[2]{meta};
        my $args = $meta->{args};

        for (qw/b b.is/) {
            ok($args->{$_}, "boolean filter arg '$_' generated");
        }
        for (qw/i i.is i.min i.xmin i.max i.xmax/) {
            ok($args->{$_}, "int filter arg '$_' generated");
        }
        for (qw/f f.is f.min f.xmin f.max f.xmax/) {
            ok($args->{$_}, "float filter arg '$_' generated");
        }
        for (qw/a a.has a.lacks/) {
            ok($args->{$_}, "array filter arg '$_' generated");
        }
        for (qw/s s.is s.contains s.not_contains s.matches s.not_matches/) {
            ok($args->{$_}, "str filter arg '$_' generated");
        }
        for (qw/s2 s2.is s2.contains s2.not_contains
                s2.matches s2.not_matches/) {
            ok(!$args->{$_}, "str filter arg '$_' NOT generated");
        }
        for (qw/s3 s3.is s3.contains s3.not_contains/) {
            ok($args->{$_}, "str filter arg '$_' generated");
        }
        for (qw/s3.matches s3.not_matches/) {
            ok(!$args->{$_}, "str filter arg '$_' NOT generated");
        }
    },
);

test_gen(
    name => 'disable search',
    table_data => [],
    table_spec => $table_spec,
    other_args => {enable_search=>0},
    status => 200,
);

test_gen(
    name => 'default_sort',
    table_data => $table_data,
    table_spec => $table_spec,
    other_args => {default_sort=>"s"},
    status => 200,
    post_test => sub {
        my ($res) = @_;
        my $func = $res->[2]{code};
        my $meta = $res->[2]{meta};
        my $args = $meta->{args};

        my $fres;
        $fres = $func->(detail=>1);
        subtest "default_sort s" => sub {
            is($fres->[0], 200, "status")
                or diag explain $fres;
            my @r = map {$_->{s}} @{$fres->[2]};
            is_deeply(\@r, [qw/a1 a2 a3 b1/], "sort result")
                or diag explain \@r;
        };
    },
);

test_gen(
    name => 'default_random',
    table_data => $table_data,
    table_spec => $table_spec,
    other_args => {default_random=>1},
    status => 200,
    post_test => sub {
        my ($res) = @_;
        my $func = $res->[2]{code};
        my $meta = $res->[2]{meta};
        my $args = $meta->{args};

        test_random_order($func, {}, 50, [qw/a1 a2 a3 b1/],
                          "sort result");
    },
);

test_gen(
    name => 'default_fields',
    table_data => $table_data,
    table_spec => $table_spec,
    other_args => {default_fields=>'s,b'},
    status => 200,
    post_test => sub {
        my ($res) = @_;
        my $func = $res->[2]{code};
        my $meta = $res->[2]{meta};
        my $args = $meta->{args};

        my $fres;
        $fres = $func->();
        subtest "default_fields s,b" => sub {
            is($fres->[0], 200, "status")
                or diag explain $fres;
            is_deeply($fres->[2], [
                ['a1', 0],
                ['b1', 0],
                ['a3', 1],
                ['a2', 1],
            ], "sort result")
                or diag explain $fres->[2];
        };
    },
);

test_gen(
    name => 'default_detail',
    table_data => $table_data,
    table_spec => $table_spec,
    other_args => {default_detail=>1},
    status => 200,
    post_test => sub {
        my ($res) = @_;
        my $func = $res->[2]{code};
        my $meta = $res->[2]{meta};
        my $args = $meta->{args};

        my $fres;
        $fres = $func->();
        subtest "default_detail 1" => sub {
            is($fres->[0], 200, "status")
                or diag explain $fres;
            is_deeply($fres->[2], $table_data, "sort result")
                or diag explain $fres->[2];
        };
    },
);

test_gen(
    name => 'default_with_field_names',
    table_data => $table_data,
    table_spec => $table_spec,
    other_args => {default_with_field_names=>0},
    status => 200,
    post_test => sub {
        my ($res) = @_;
        my $func = $res->[2]{code};
        my $meta = $res->[2]{meta};
        my $args = $meta->{args};

        my $fres;
        $fres = $func->(fields=>['s', 'b']);
        subtest "default_with_field_names 0" => sub {
            is($fres->[0], 200, "status")
                or diag explain $fres;
            is_deeply($fres->[2],
                      [['a1', 0],
                       ['b1', 0],
                       ['a3', 1],
                       ['a2', 1]],
                      "sort result")
                or diag explain $fres->[2];
        };
    },
);

test_gen(
    name => 'default_result_limit',
    table_data => $table_data,
    table_spec => $table_spec,
    other_args => {default_result_limit=>2},
    status => 200,
    post_test => sub {
        my ($res) = @_;
        my $func = $res->[2]{code};

        test_query($func, {}, 2, 'default result_limit');
        test_query($func, {result_limit=>3}, 3, 'explicit result_limit');
    },
);

DONE_TESTING:
done_testing();
