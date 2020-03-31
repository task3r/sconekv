@0x85f35d96740c57e8;

using Java = import "/java.capnp";
$Java.package("pt.ulisboa.tecnico.sconekv.common.transport");
$Java.outerClassname("ClientRequest");

struct RequestMessage {
  id @0 :UInt64;

  union {
      write @1 :Write;
      read @2 :Read;
      commit @3 :Commit;
      sync @4 :Void; # talvez no futuro passe a ter um type
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
    coords @0 :List(Node);
    ops @1 :List(TxOperation);
}

struct Node {
    id @0 :UInt64;
}

struct TxOperation {
    union {
        write @0 :Write;
        read @1 :Read;
    }
    version @2 :Int16;
}
