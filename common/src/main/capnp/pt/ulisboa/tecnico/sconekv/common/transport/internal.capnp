@0xea3d74889c517ffa;

using Java = import "/java.capnp";
using Request = import "external.capnp".Request;
using ID = import "common.capnp".ID;

$Java.package("pt.ulisboa.tecnico.sconekv.common.transport");
$Java.outerClassname("Internal");

struct ViewNumber {
    timestamp @0: UInt64;
    messageId @1: ID;
}

struct InternalMessage {
    viewVersion @0: ViewNumber;
    union {
        prepare @1: Prepare;
        prepareOk @2: PrepareOK;
    }
}

struct Prepare {
    message @0: Request;
    opNumber @1: Int64;
    commitNumber @2: Int64;
    bucket @3: Int16;
}

struct PrepareOK {
    opNumber @0: Int64;
    id @1: ID;
    bucket @2: Int16;
}
