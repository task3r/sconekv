package pt.ulisboa.tecnico.sconekv.common.transport;

import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;

public class Response {

    public enum ResponseType {
        OK(ServerResponse.ResponseMessage.ResponseType.OK),
        NOK(ServerResponse.ResponseMessage.ResponseType.NOK);

        ServerResponse.ResponseMessage.ResponseType rt;

        ResponseType(ServerResponse.ResponseMessage.ResponseType responseType) {
            this.rt = responseType;
        }

        ServerResponse.ResponseMessage.ResponseType serialize() {
            return this.rt;
        }
    }

    ResponseType responseType;

    public Response(ResponseType responseType) {
        this.responseType = responseType;
    }

    public Response(MessageReader responseMessage) {
        ServerResponse.ResponseMessage.ResponseType responseType = responseMessage.getRoot(ServerResponse.ResponseMessage.factory).getContent();
        switch (responseType) {
            case OK:
                this.responseType = ResponseType.OK;
                break;
            case NOK:
                this.responseType = ResponseType.NOK;
                break;
        }
    }

    public MessageBuilder serialize() {
        MessageBuilder messageBuilder = new MessageBuilder();
        messageBuilder.initRoot(ServerResponse.ResponseMessage.factory).setContent(responseType.serialize());
        return messageBuilder;
    }
}
