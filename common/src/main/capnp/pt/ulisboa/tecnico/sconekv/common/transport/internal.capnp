@0xea3d74889c517ffa;

using Java = import "/java.capnp";
using Request = import "external.capnp".Request;
using ID = import "common.capnp".ID;
using Node = import "common.capnp".Node;
using ViewVersion = import "common.capnp".ViewVersion;

$Java.package("pt.ulisboa.tecnico.sconekv.common.transport");
$Java.outerClassname("Internal");


struct InternalMessage {
    viewVersion @0 :ViewVersion;
    node @1 :Node;
    union {
        prepare @2 :Prepare;
        prepareOk @3 :PrepareOK;
        startViewChange @4 :Void;
        doViewChange @5 :DoViewChange;
        startView @6 :StartView;
        getState @7 :GetState;
        newState @8 :NewState;
    }
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
    log @0 :List(LoggedRequest);
    term @1 :ViewVersion;
    commitNumber @2 :Int32;
}

struct StartView {
    log @0 :List(LoggedRequest);
    commitNumber @1 :Int32;
}

struct GetState {
    opNumber @0 :Int32;
}

struct NewState {
    logSegment @0 :List(LoggedRequest);
    opNumber @1 :Int32;
    commitNumber @2 :Int32;
}

struct LoggedRequest {
    request @0: Request; # I don't like this, but capnproto is picky with lists and I couldn't set an index to an existing reader, this is the work around
}
