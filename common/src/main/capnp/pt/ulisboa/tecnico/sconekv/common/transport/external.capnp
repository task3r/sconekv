@0x8ec0ed4ff0a7657a;

using Java = import "/java.capnp";
using TransactionID = import "common.capnp".TransactionID;
using Operation = import "common.capnp".Operation;
using DHT = import "common.capnp".DHT;

$Java.package("pt.ulisboa.tecnico.sconekv.common.transport");
$Java.outerClassname("External");

struct Request {
    txID @0 :TransactionID;

    union {
      write @1 :Data; # read or write simply sends the key
      read @2 :Data;
      commit @3 :Commit;
      getDht @4 :Void;
    }
}

struct Commit {
    buckets @0 :List(Int16);
    ops @1 :List(Operation);
}

struct Response {
    txID @0 :TransactionID; # same as the request

    union {
        write @1 :WriteResponse;
        read @2 :ReadResponse;
        commit @3 :CommitResponse;
        dht @4 :DHT;
        ack @5 :Void;
    }
}

struct WriteResponse {
    key @0 :Data;
    version @1 :Int16;
}

struct ReadResponse {
    key @0 :Data;
    value @1 :Data;
    version @2 :Int16;
}

struct CommitResponse {
    result @0 :Result;
    # maybe include a message? for debug of failures in distributed tx

    enum Result {
        ok @0;
        nok @1;
    }
}
