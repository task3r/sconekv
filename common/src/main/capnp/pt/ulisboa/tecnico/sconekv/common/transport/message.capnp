@0x8ec0ed4ff0a7657a;

using Java = import "/java.capnp";
$Java.package("pt.ulisboa.tecnico.sconekv.common.transport");
$Java.outerClassname("Message");


struct Request {
    id @0 :UInt64;

    union {
      write @1 :Write;
      read @2 :Read;
      commit @3 :Commit;
    }
}

struct Write {
    key @0 :Data;
    value @1 :Data;
}

struct Read {
    key @0 :Data;
}

struct Commit {
    buckets @0 :List(UInt16);
    ops @1 :List(Operation);
}

struct Node {
    id @0 :UInt64;
}

struct Operation {
    union {
        write @0 :Write;
        read @1 :Read;
    }
    version @2 :Int16;
}

struct Response {
    id @0 :UInt64; # same as the request

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
