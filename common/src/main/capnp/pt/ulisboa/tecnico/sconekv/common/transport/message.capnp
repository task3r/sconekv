@0x8ec0ed4ff0a7657a;

using Java = import "/java.capnp";
$Java.package("pt.ulisboa.tecnico.sconekv.common.transport");
$Java.outerClassname("Message");

struct TransactionID {
    mostSignificant @0: UInt64;
    leastSignificant @1: UInt64;
    localID @2: UInt32;
}

struct Request {
    txID @0 :TransactionID;

    union {
      write @1 :Data; # read or write simply sends the key
      read @2 :Data;
      commit @3 :Commit;
    }
}

struct Commit {
    buckets @0 :List(UInt16);
    ops @1 :List(Operation);
}

struct Operation {
    union {
        write @0 :Data; # value written
        read @1 :Void;
    }
    key @2 :Data;
    version @3 :Int16;
}

struct Response {
    txID @0 :TransactionID; # same as the request

    union {
        write @1 :WriteResponse;
        read @2 :ReadResponse;
        commit @3 :CommitResponse;
        ack @4 :Void;
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
