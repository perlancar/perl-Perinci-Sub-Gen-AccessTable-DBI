package Perinci::Sub::Gen::AccessTable::DBI;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use experimental 'smartmatch';
use Log::Any '$log';

use Function::Fallback::CoreOrPP qw(clone);
use Locale::TextDomain::UTF8 'Perinci-Sub-Gen-AccessTable-DBI';
use DBI;
use Perinci::Sub::Gen::AccessTable qw(gen_read_table_func);
use Perinci::Sub::Util qw(gen_modified_sub);

require Exporter;
our @ISA       = qw(Exporter);
our @EXPORT_OK = qw(gen_read_dbi_table_func);

my $label = "(gen_read_dbi_table_func)";

sub __parse_schema {
    require Data::Sah::Normalize;
    Data::Sah::Normalize::normalize_schema($_[0]);
}

gen_modified_sub(
    output_name => 'gen_read_dbi_table_func',
    install_sub => 0,
    base_name   => 'Perinci::Sub::Gen::AccessTable::gen_read_table_func',
    summary     => 'Generate function (and its metadata) to read DBI table',
    description => <<'_',

The generated function acts like a simple single table SQL SELECT query,
featuring filtering, ordering, and paging, but using arguments as the 'query
language'. The generated function is suitable for exposing a table data from an
API function. Please see Perinci::Sub::Gen::AccessTable's documentation for more
details on what arguments the generated function will accept.

_
    remove_args => ['table_data'],
    add_args    => {
        table_name => {
            req => 1,
            schema => 'str*',
            summary => 'DBI table name',
        },
        dbh => {
            schema => 'obj*',
            summary => 'DBI database handle',
        },
    },
    modify_args => {
        table_spec => sub {
            my $as = shift;
            $as->{description} = <<'_';

Just like Perinci::Sub::Gen::AccessTable's table_spec, except that each field
specification can have a key called `db_field` to specify the database field (if
different). Currently this is required. Future version will be able to generate
table_spec from table schema if table_spec is not specified.

_
        },
    },
    modify_meta => sub {
        my $meta = shift;
        push @{ $meta->{'x.dynamic_generator_modules'} }, __PACKAGE__;
    },
);
sub gen_read_dbi_table_func {
    my %args = @_;

    # XXX schema
    my $table_name = $args{table_name}; delete $args{table_name};
    $table_name or return [400, "Please specify table_name"];
    my $dbh = $args{dbh}; delete $args{dbh};
    $dbh or return [400, "Please specify dbh"];

    # duplicate and make each field's schema normalized
    my $table_spec = clone($args{table_spec});
    for my $fspec (values %{$table_spec->{fields}}) {
        $fspec->{schema} //= 'any';
        $fspec->{schema} = __parse_schema($fspec->{schema});
    }

    my $table_data = sub {
        my $query = shift;

        my ($db) = $dbh->get_info(17);
        unless ($db =~ /\A(SQLite|mysql|Pg)\z/) {
            $log->warnf("$label Database is not supported: %s", $db);
        }

        # function to quote identifier, e.g. `col` or "col"
        my $qi = sub {
            if ($db =~ /SQLite|mysql/) { return "`$_[0]`" }
            return qq("$_[0]");
        };

        my $fspecs = $table_spec->{fields};
        my @fields = keys %$fspecs;
        my @searchable_fields = grep {
            !defined($fspecs->{$_}{searchable}) || $fspecs->{$_}{searchable}
        } @fields;

        my $filtered;
        my @wheres;
        # XXX case_insensitive_search & word_search not yet observed
        my $q = $query->{q};
        if (defined($q) && @searchable_fields) {
            push @wheres, "(".
                join(" OR ", map {$qi->($fspecs->{$_}{db_field}//$_)." LIKE ".
                                      $dbh->quote("%$q%")}
                         @searchable_fields).
                    ")";
        }
        if ($args{custom_search}) {
            $filtered = 0; # perigen-acctbl will be doing custom_search
        }
        if ($args{custom_filter}) {
            $filtered = 0; # perigen-acctbl will be doing custom_search
        }
        for my $filter (@{$query->{filters}}) {
            my ($f, $ftype, $op, $opn) = @$filter;
            my $qdbf = $qi->($fspecs->{$f}{db_field} // $f);
            my $qopn = $dbh->quote($opn);
            if ($op eq 'truth')     { push @wheres, $qdbf
            } elsif ($op eq '~~')   { $filtered = 0 # not supported
            } elsif ($op eq '!~~')  { $filtered = 0 # not supported
            } elsif ($op eq 'eq')   { push @wheres, "$qdbf = $qopn"
            } elsif ($op eq '==')   { push @wheres, "$qdbf = $qopn"
            } elsif ($op eq 'ne')   { push @wheres, "$qdbf <> $qopn"
            } elsif ($op eq '!=')   { push @wheres, "$qdbf <> $qopn"
            } elsif ($op eq 'ge')   { push @wheres, "$qdbf >= $qopn"
            } elsif ($op eq '>=')   { push @wheres, "$qdbf >= $qopn"
            } elsif ($op eq 'gt')   { push @wheres, "$qdbf > $qopn"
            } elsif ($op eq '>' )   { push @wheres, "$qdbf > $qopn"
            } elsif ($op eq 'le')   { push @wheres, "$qdbf <= $qopn"
            } elsif ($op eq '<=')   { push @wheres, "$qdbf <= $qopn"
            } elsif ($op eq 'lt')   { push @wheres, "$qdbf < $qopn"
            } elsif ($op eq '<' )   { push @wheres, "$qdbf < $qopn"
            } elsif ($op eq '=~')   { $filtered = 0 # not supported
            } elsif ($op eq '!~')   { $filtered = 0 # not supported
            } elsif ($op eq 'pos')  { $filtered = 0 # different substr funcs
            } elsif ($op eq '!pos') { $filtered = 0 # different substr funcs
            } elsif ($op eq 'call') { $filtered = 0 # not supported
            } else {
                die "BUG: Unknown op $op";
            }
        }
        $filtered //= 1;

        my $sorted;
        my @orders;
        if ($query->{random}) {
            push @orders, "RANDOM()";
        } elsif (@{$query->{sorts}}) {
            for my $s (@{$query->{sorts}}) {
                my ($f, $op, $desc) = @$s;
                push @orders, $qi->($fspecs->{$f}{db_field} // $f).
                    ($desc == -1 ? " DESC" : "");
            }
        }
        $sorted //= 1;

        my $paged;
        my $limit = "";
        my ($ql, $qs) = ($query->{result_limit}, $query->{result_start}-1);
        if (defined($ql) || $qs > 0) {
            $limit = join(
                "",
                "LIMIT ".($ql // ($db eq 'Pg' ? "ALL":"999999999")),
                ($qs > 1 ? ($db eq 'mysql' ? ",$qs" : " OFFSET $qs") : "")
            );
        }
        $paged //= 1;

        my $sql = join(
            "",
            "SELECT ",
            join(",", map {$qi->($fspecs->{$_}{db_field}//$_)." AS ".$qi->($_)}
                     @{$query->{requested_fields}}).
                         " FROM ".$qi->($table_name),
            (@wheres ? " WHERE ".join(" AND ", @wheres) : ""),
            (@orders ? " ORDER BY ".join(",", @orders) : ""),
            $limit,
        );
        $log->tracef("$label SQL=%s", $sql);

        my $sth = $dbh->prepare($sql);
        $sth->execute or die "Can't query: ".$sth->errstr;
        my @r;
        while (my $row = $sth->fetchrow_hashref) { push @r, $row }

        {data=>\@r, paged=>$paged, filtered=>$filtered, sorted=>$sorted,
             fields_selected=>0, # XXX i'm lazy to handle detail=0
         };
    };

    @_ = (%args, table_data => $table_data);
    goto &gen_read_table_func;
}

1;
# ABSTRACT:

=head1 SYNOPSIS

Your database table C<countries>:

 | id | eng_name                 | ind_name        |
 |----+--------------------------+-----------------|
 | cn | China                    | Cina            |
 | id | Indonesia                | Indonesia       |
 | sg | Singapore                | Singapura       |
 | us | United States of America | Amerika Serikat |

In list_countries.pl:

 #!perl
 use strict;
 use warnings;
 use Perinci::CmdLine;
 use Perinci::Sub::Gen::AccessTable::DBI qw(gen_read_dbi_table_func);

 our %SPEC;

 my $res = gen_read_dbi_table_func(
     name        => 'list_countries',
     summary     => 'func summary',     # opt
     description => 'func description', # opt
     dbh         => ...,
     table_name  => 'countries',
     table_spec  => {
         summary => 'List of countries',
         fields => {
             id => {
                 schema => 'str*',
                 summary => 'ISO 2-letter code for the country',
                 index => 0,
                 sortable => 1,
             },
             eng_name => {
                 schema => 'str*',
                 summary => 'English name',
                 index => 1,
                 sortable => 1,
             },
             ind_name => {
                 schema => 'str*',
                 summary => 'Indonesian name',
                 index => 2,
                 sortable => 1,
             },
         },
         pk => 'id',
     },
 );
 die "Can't generate function: $res->[0] - $res->[1]" unless $res->[0] == 200;

 Perinci::CmdLine->new(url=>'/main/list_countries')->run;

Now you can do:

 # list all countries, by default only PK field is shown
 $ list_countries.pl --format=text-simple
 cn
 id
 sg
 us

 # show as json, randomize order
 $ list_countries.pl --format=json --random
 ["id","us","sg","cn"]

 # only list countries which contain 'Sin', show all fields (--detail)
 $ list_countries.pl --q=Sin --detail
 .----------------------------.
 | eng_name  | id | ind_name  |
 +-----------+----+-----------+
 | Singapore | sg | Singapura |
 '-----------+----+-----------+

 # show only certain fields, limit number of records, return in YAML format
 $ list_countries.pl --fields '[id, eng_name]' --result-limit 2 --format=yaml
 - 200
 - OK
 -
   - id: cn
     eng_name: China
   - id: id
     eng_name: Indonesia


=head1 DESCRIPTION

This module is just like L<Perinci::Sub::Gen::AccessTable>, except that table
data source is from DBI. gen_read_dbi_table_func() accept mostly the same
arguments as gen_read_table_func(), except: 'table_name' instead of
'table_data', and 'dbh'.

Supported databases: SQLite, MySQL, PostgreSQL.

Early versions tested on: SQLite.


=head1 CAVEATS

It is often not a good idea to expose your database schema directly as API.


=head1 FAQ


=head1 SEE ALSO

L<Perinci::Sub::Gen::AccessTable>

=cut
