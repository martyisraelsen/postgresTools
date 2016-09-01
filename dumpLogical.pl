#!/usr/bin/perl
## Author:  Marty Israelsen (marty.israelsen@usurf.usu.edu)
## Date:    Aug 26th 2016

use strict;
use DBI;
use Getopt::Long;
use JSON::Parse ':all';
use Switch;

my $OUTFILE     =   "&STDOUT";
my $SLOT_NAME   =  "test_slot1";
my $PEEK        =   0;
my $DB          =   "postgres";
my $HOST        =   "localhost";
my $PORT        =   '5432';
my $USERNAME    =   "postgres";
my $PASSWD      =   "";
my %PRIMARY_KEY_HASH;

##################################
# Set up command line parameters #
##################################
GetOptions ("slot=s"            =>      \$SLOT_NAME,
            "outputFile=s"      =>      \$OUTFILE,
            "peek"              =>      \$PEEK,
            "db=s"              =>      \$DB,
            "host=s"            =>      \$HOST,
            "port=i"            =>      \$PORT,
            "user=s"            =>      \$USERNAME,
            "passwd=s"          =>      \$PASSWD)
 or die("Error in command line arguments\n");

##################################
# Set up database connection     #
##################################
sub dbierrorHandler() {
        print "\n\nDBI ERROR = $DBI::errstr\n\n\n";
        exit;
}

my $dbh = DBI->connect("DBI:Pg:dbname = $DB; host = $HOST; port=$PORT","$USERNAME", "$PASSWD",
    {
        RaiseError => 0,
        HandleError =>\&dbierrorHandler
    }
);

exit unless $dbh;


#################################################################
# Start out by opening outputfile and calling dumpSlotChanges() #
#################################################################
open(OUT, ">$OUTFILE") or die "Cannot open output file\n";
&dumpSlotChanges($SLOT_NAME);


####################################################################################
# dumpSlotChanges - dumps all changes as SQL INSERT/UPDATE/DELETE commands to file #
####################################################################################
sub dumpSlotChanges {
    my $slot = shift;
    my $data;
    my $action;
    my $trans;
    my $schemaTable;
    my $get;

    my $get =($PEEK ? "peek" : "get");

    my $sth = $dbh->prepare("SELECT * FROM pg_logical_slot_$get\_changes(
            '$slot', NULL, NULL,
            'min_proto_version', '1',
            'max_proto_version', '1',
            'startup_params_format', '1',
            'proto_format', 'json',
            'no_txinfo', 't'
            );");
    $sth->execute();

    # Get all "Primary Keys" and put into a hash (by table name) for later use
    #   Note:  If the table has a multi-component Primary Key we comma delimit them.
    my $sth2 = $dbh->prepare("SELECT c.column_name, tc.table_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
            JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
            WHERE constraint_type = 'PRIMARY KEY';");
    $sth2->execute();
    while (my $ref = $sth2->fetchrow_hashref) {
        if ($PRIMARY_KEY_HASH{$ref->{table_name}} eq "") {
            $PRIMARY_KEY_HASH{$ref->{table_name}} = $ref->{column_name};
        } else {
            $PRIMARY_KEY_HASH{$ref->{table_name}} .= ",".$ref->{column_name};
        }
    }

    while (my $ref = $sth->fetchrow_hashref) {
        if (valid_json ($ref->{data})) {
            $data = parse_json ($ref->{data});
        } else {
            print "ERROR:  Your JSON was invalid: $ref->{data}\n";
            next;
        }

        $schemaTable = $data->{relation}[0] . "\." . $data->{relation}[1];
        $action = $data->{action};

        # Now, build up a transaction into a buffer "$trans" while dealing with each action
        #    When the COMMIT happens we write out the $trans buffer.
        switch ($action) {
            case "S"    {                                                       }
            case "B"    {   $trans = "\nBEGIN;\n";                              }
            case "I"    {   $trans .= &createInsert($schemaTable,$data);        }
            case "U"    {   $trans .= &createUpdate($schemaTable,$data);        }
            case "D"    {   $trans .= &createDelete($schemaTable,$data);        }
            case "C"    {
                            $trans .= "COMMIT;\n\n";
                            &dumpItOut($trans);
                        }

            else        {   print "\n\n*** UNKNOWN ACTION $data->{action} ***\n\n";}
        }
    }

    print "\n\nDONE...  output written to $OUTFILE\n\n";
}

sub createInsert {
    my $schemaTable = shift;
    my $data = shift;
    my $insert;
    my $vals;

    $insert = "INSERT INTO $schemaTable (";

    my %dataHash = %{$data->{newtuple}};
    foreach my $key (reverse(keys %dataHash)) {
        $insert .= "$key,";
        $vals  .= &quoteIfStr($dataHash{$key}).",";
    }
    $insert =~ s/\,$//;
    $vals  =~ s/\,$//;

    $insert .= ") VALUES ($vals);\n";

    return $insert;
}

sub createUpdate {
    my $schemaTable = shift;
    my $data = shift;
    my $update;
    my $where;
    my $schema;
    my $key;
    my $table;

    # Get Primary key of table.
    ($schema,$table) = split(/\./,$schemaTable);
    my @pkey = split(/,/,$PRIMARY_KEY_HASH{$table});

    # Create UPDATE string
    $update = "UPDATE $schemaTable SET ";

    my %dataHash = %{$data->{newtuple}};
    foreach $key (reverse(keys %dataHash)) {
        $update .= "$key = ".&quoteIfStr($dataHash{$key})." ,";
    }
    $update =~ s/\,$//;

    # Create WHERE part of string
    $update .= " WHERE ";
    foreach $key (@pkey) {
        $update .= "  $key = ".&quoteIfStr($dataHash{$key})." AND ";
    }
    $update =~ s/ AND $/;\n/;

    return $update;
}

sub createDelete {
    my $schemaTable = shift;
    my $data = shift;
    my $delete;
    my $schema;
    my $table;
    my $key;
    my %dataHash = %{$data->{oldtuple}};

    ($schema,$table) = split(/\./,$schemaTable);
    my @pkey = split(/,/,$PRIMARY_KEY_HASH{$table});

    my %dataHash = %{$data->{oldtuple}};
    $delete = "DELETE FROM $schemaTable WHERE ";
    foreach $key (@pkey) {
        $delete .= "  $key = ".&quoteIfStr($dataHash{$key})." AND ";
    }
    $delete =~ s/ AND $/;\n/;

    return $delete;
}

sub quoteIfStr {
    my $val = shift;
    if ($val + 0 ne $val) {
        return "'$val'";
    }
    return $val;
}

sub dumpItOut {
    my $in = shift;
    print OUT $in;
}
