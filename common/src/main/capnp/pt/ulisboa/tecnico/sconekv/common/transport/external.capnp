@0x8ec0ed4ff0a7657a;

using Java = import "/java.capnp";
using TransactionID = import "common.capnp".TransactionID;
using Operation = import "common.capnp".Operation;
using Transaction = import "common.capnp".Transaction;
using DHT = import "common.capnp".DHT;

$Java.package("pt.ulisboa.tecnico.sconekv.common.transport");
$Java.outerClassname("External");

struct Request {
    txID @0 :TransactionID;

    union {
      write @1 :Data; # read or write simply sends the key
      read @2 :Data;
      delete @3 :Data;
      commit @4 :Transaction;
      getDht @5 :Void;
    }
}

struct Response {
    txID @0 :TransactionID; # same as the request

    union {
        write @1 :WriteResponse;
        read @2 :ReadResponse;
        delete @3 :DeleteResponse;
        commit @4 :CommitResponse;
        dht @5 :DHT;
    }
}

struct WriteResponse {
    key @0 :Data;
    version @1 :Int16;
}

struct DeleteResponse {
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
