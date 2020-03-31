@0xa2f5e47acf1fce32;

using Java = import "/java.capnp";
$Java.package("pt.ulisboa.tecnico.sconekv.common.transport");
$Java.outerClassname("ServerResponse");

struct ResponseMessage {
    content @0 :ResponseType;

    enum ResponseType {
        ok @0;
        nok @1;
    }
}


