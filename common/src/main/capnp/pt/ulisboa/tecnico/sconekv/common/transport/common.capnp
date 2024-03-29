@0x832b26059281d4bb;

using Java = import "/java.capnp";

$Java.package("pt.ulisboa.tecnico.sconekv.common.transport");
$Java.outerClassname("Common");

struct Node {
    id @0 :ID;
    address @1 :Data;
}

struct ViewVersion {
    timestamp @0 :Int64;
    messageId @1 :ID;
}

struct DHT {
    version @0 :ViewVersion;
    nodes @1 :List(Node);
    numBuckets @2 :Int16;
    murmurSeed @3 :Int32;
}

struct ID {
    mostSignificant @0 :Int64;
    leastSignificant @1 :Int64;
}

struct Operation {
    union {
        write @0 :Data; # value written
        read @1 :Void;
        delete @2 :Void;
    }
    key @3 :Data;
    version @4 :Int16;
}

struct TransactionID {
    clientID @0 :ID;
    localID @1 :Int32;
}

struct Transaction {
    buckets @0 :List(Int16);
    ops @1 :List(Operation);
}
