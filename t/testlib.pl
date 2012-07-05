use 5.010;
use strict;
use warnings;
use Log::Any '$log';

use Perinci::Sub::Gen::AccessTable qw(gen_read_table_func);
use Test::More 0.96;

sub test_gen {
    my (%args) = @_;

    subtest $args{name} => sub {
        my $res;
        my %fargs = (
            table_data => $args{table_data},
            table_spec => $args{table_spec},
        );
        if ($args{other_args}) {
            while (my ($k, $v) = each %{$args{other_args}}) {
                $fargs{$k} = $v;
            }
        }
        eval { $res = gen_read_table_func(%fargs) };
        my $eval_err = $@;
        diag "died during function: $eval_err" if $eval_err;

        if ($args{dies}) {
            ok($eval_err, "dies");
        }

        if ($args{status}) {
            is($res->[0], $args{status}, "status = $args{status}") or
                do { diag explain $res; return };
        }

        if ($res->[0] == 200) {
            my $func = $res->[2]{code};
            my $meta = $res->[2]{meta};
            is(ref($func), 'CODE', 'func returned');
            is(ref($meta), 'HASH', 'meta returned');
            my $args = $meta->{args};
            for my $a (qw/with_field_names detail fields
                          sort random result_limit result_start
                         /) {
                ok($args->{$a}, "common arg '$a' generated");
            }
            if (!defined($fargs{enable_search}) || $fargs{enable_search}) {
                ok( $args->{q}, "search arg 'q' generated");
            } else {
                ok(!$args->{q}, "search arg 'q' not generated");
            }
        }

        if ($args{post_test}) {
            $args{post_test}->($res);
        }
    };
}

sub gen_test_data {
    my ($aoa_data) = @_;

    my $table_data = [
        {s=>'a1', s2=>'', s3=>'a' , i=>1 , f=>0.1, a=>[qw//]     , b=>0},
        {s=>'b1', s2=>'', s3=>'aa', i=>2 , f=>0.2, a=>[qw/t2/]   , b=>0},
        {s=>'a3', s2=>'', s3=>'aa', i=>4 , f=>1.1, a=>[qw/t1 t2/], b=>1},
        {s=>'a2', s2=>'', s3=>'a' , i=>-3, f=>1.2, a=>[qw/t1/]   , b=>1},
    ];
    if ($aoa_data) {
        for my $r (@$table_data) {
            $r = [
                $r->{s}, $r->{s2}, $r->{s3},
                $r->{i}, $r->{f},  $r->{a},  $r->{b},
            ];
        }
    }

    my $table_spec = {
        fields => {
            s  => {schema=>'str*'   , index=>0, filterable_regex=>1, },
            s2 => {schema=>'str*'   , index=>1, filterable=>0, },
            s3 => {schema=>'str*'   , index=>2, },
            i  => {schema=>'int*'   , index=>3, },
            f  => {schema=>'float*' , index=>4, },
            a  => {schema=>'array*' , index=>5, sortable=>0, },
            b  => {schema=>'bool*'  , index=>6, },
        },
        pk => 's',
    };

    return ($table_data, $table_spec);
}

sub test_random_order {
    my ($func, $args, $n, $elems, $test_name) = @_;

    my @x;
    for (1 .. $n) {
        my $a = $func->(%$args)->[2];
        push @x, $a->[0] unless $a->[0] ~~ @x;
    }

    is_deeply([sort {$a cmp $b} @x],
              [sort {$a cmp $b} @$elems], "random order ($n runs)")
        or diag explain \@x;
}

sub test_query {
    my ($func, $args, $test, $name) = @_;

    my $res = $func->(%$args);
    subtest $name => sub {
        is($res->[0], 200, "status = 200")
            or diag explain $res;
        if (ref($test) eq 'CODE') {
            $test->($res->[2]);
        } else {
            is(scalar(@{$res->[2]}), $test, "num_results = $test")
                or diag explain $res->[2];
        }
    };

    $res->[2];
}

1;
