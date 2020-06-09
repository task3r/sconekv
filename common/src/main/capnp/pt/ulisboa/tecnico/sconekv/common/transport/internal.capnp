@0xea3d74889c517ffa;

using Java = import "/java.capnp";
using ID = import "common.capnp".ID;
using TransactionID = import "common.capnp".TransactionID;
using Transaction = import "common.capnp".Transaction;
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
        localDecisionResponse @9 :LocalDecisionResponse;
        requestRollbackLocalDecision @10 :TransactionID;
        rollbackLocalDecisionResponse @11 :TransactionID;
        commitTransaction @12 :TransactionID;
        abortTransaction @13 :TransactionID;
    }
}

struct LogEvent {
    txID @0 :TransactionID;
    union {
        transaction @1 :LoggedTransaction;
        decision @2 :Bool;
    }
}

struct LoggedTransaction {
    transaction @0 : Transaction;
    prepared @1 :Bool;
}

struct Prepare {
    event @0 :LogEvent;
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
    log @0 :List(LoggedEvent);
    term @1 :ViewVersion;
    commitNumber @2 :Int32;
}

struct StartView {
    log @0 :List(LoggedEvent);
    commitNumber @1 :Int32;
}

struct GetState {
    opNumber @0 :Int32;
}

struct NewState {
    logSegment @0 :List(LoggedEvent);
    opNumber @1 :Int32;
    commitNumber @2 :Int32;
}

struct LoggedEvent {
    event @0 :LogEvent; # I don't like this, but capnproto is picky with lists and I couldn't set an index to an existing reader, this is the work around
}

struct LocalDecisionResponse {
    txID @0 :TransactionID;
    toCommit @1 :Bool;
}
