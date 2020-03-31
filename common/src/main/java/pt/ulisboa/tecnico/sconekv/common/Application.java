package pt.ulisboa.tecnico.sconekv.common;

import java.io.IOException;

import pt.ulisboa.tecnico.sconekv.common.transport.Request;
import pt.ulisboa.tecnico.sconekv.common.transport.Response;


public class Application {

    public static Response sendRequest(Request request) throws IOException {
//        MessageBuilder message = new MessageBuilder();
//        Msg.Builder msgbuilder = message.initRoot(Msg.factory);
//
//        msgbuilder.initContent().initCena().setBody("mas agora");
//        Types.Coisa.Builder coisa = msgbuilder.initContent().initCoisa();
//        coisa.setBody("coisas");
//        coisa.setAnothaBody("ainda");
//
//        SerializePacked.writeToUnbuffered((new FileOutputStream(file)).getChannel(), message);
        return null;
    }

    public static Request receiveRequest() throws IOException {
//        MessageReader message = SerializePacked.readFromUnbuffered((new FileInputStream(file)).getChannel());
//
//        Request.Reader request = message.getRoot(Request.factory);
//
//        Serialize.write();
//
//        switch (request.which()) {
//            case READ:
//                break;
//                case
//        }
        return null;
    }

    public static void main(String[] args) {
    }
}
