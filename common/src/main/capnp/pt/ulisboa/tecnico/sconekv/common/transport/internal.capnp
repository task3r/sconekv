@0xea3d74889c517ffa;

using Java = import "/java.capnp";
using Request = import "external.capnp".Request;
using ID = import "common.capnp".ID;
using Node = import "common.capnp".Node;
using ViewVersion = import "common.capnp".ViewVersion;
using Transaction = import "common.capnp".Transaction;

$Java.package("pt.ulisboa.tecnico.sconekv.common.transport");
$Java.outerClassname("Internal");


struct InternalMessage {
    viewVersion @0 :ViewVersion;
    union {
        prepare @1 :Prepare;
        prepareOk @2 :PrepareOK;
        startViewChange @3 :Void;
        doViewChange @4 :DoViewChange;
        startView @5 :StartView;
    }
    node @6 :Node;
}

struct Prepare {
    message @0 :Request;
    opNumber @1 :Int32;
    commitNumber @2 :Int32;
    bucket @3 :Int16;
}

struct PrepareOK {
    opNumber @0 :Int32;
    id @1 :ID;
    bucket @2 :Int16;
}

struct DoViewChange {
    log @0 :List(Transaction);
    term @1 :ViewVersion;
    commitNumber @2 :Int32;
}

struct StartView {
    log @0 :List(Transaction);
    commitNumber @1 :Int32;
}
