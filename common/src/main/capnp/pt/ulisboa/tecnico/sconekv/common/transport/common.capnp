@0x832b26059281d4bb;

using Java = import "/java.capnp";

$Java.package("pt.ulisboa.tecnico.sconekv.common.transport");
$Java.outerClassname("Common");

struct ID {
    mostSignificant @0: UInt64;
    leastSignificant @1: UInt64;
}

struct Operation {
    union {
        write @0 :Data; # value written
        read @1 :Void;
    }
    key @2 :Data;
    version @3 :Int16;
}

struct TransactionID {
    clientID @0: ID;
    localID @1: UInt32;
}
