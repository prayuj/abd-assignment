package edu.sjsu.cs249.abd;
import edu.sjsu.cs249.abd.Grpc.*;
import io.grpc.stub.StreamObserver;

import javax.naming.Name;
import java.util.HashMap;

public class ServerService extends edu.sjsu.cs249.abd.ABDServiceGrpc.ABDServiceImplBase{
    HashMap<Long,HashMap<String,Long>> registers = new HashMap<>();
    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> responseObserver){
        HashMap<String,Long> data = new HashMap<>();
        data.put("value", request.getValue());
        data.put("label", request.getLabel());
        this.registers.put(request.getAddr(),data);
        printRegisterValues();
        responseObserver.onNext(WriteResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
    public void read1(Read1Request request, StreamObserver<Read1Response> responseObserver) {
        long registerAddress = request.getAddr();
        if(!this.registers.containsKey(registerAddress)) {
            responseObserver.onNext(Read1Response.newBuilder()
                    .setRc(1)
                    .build());
            responseObserver.onCompleted();
            return;
        }
        HashMap<String,Long> data = this.registers.get(registerAddress);
        long value = data.get("value");
        long label = data.get("label");
        responseObserver.onNext(Read1Response.newBuilder()
                .setRc(0)
                .setValue(value)
                .setLabel(label)
                .build());
        responseObserver.onCompleted();
    }
    public void read2(Read2Request request, StreamObserver<Read2Response> responseObserver) {
        long registerAddress = request.getAddr();
        long label = request.getLabel();
        long value = request.getValue();
        HashMap<String,Long> data = new HashMap<>();

        if (!this.registers.containsKey(registerAddress)) {
            data.put("value", request.getValue());
            data.put("label", request.getLabel());
            this.registers.put(request.getAddr(),data);
            printRegisterValues();
            responseObserver.onNext(Read2Response.newBuilder().build());
            responseObserver.onCompleted();
        } else {
            HashMap<String,Long> storedData = this.registers.get(registerAddress);
            long storedDataLabel = storedData.get("label");
            if (storedDataLabel > label) {
                data.put("value", value);
                data.put("label", label);
                this.registers.put(registerAddress,data);
                printRegisterValues();
                responseObserver.onNext(Read2Response.newBuilder().build());
                responseObserver.onCompleted();
            }
        }
    }

    public void name(NameRequest request, StreamObserver<NameResponse> responseObserver){
        responseObserver.onNext(NameResponse.newBuilder().
                    setName("Prayuj").build());
        responseObserver.onCompleted();
    }
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver){
        System.exit(0);
    }

    public void delayFor(int seconds) {
        long now = System.currentTimeMillis();
        while (System.currentTimeMillis() - now < seconds * 1000);
    }

    private void printRegisterValues(){
        System.out.println("Current register values: " + this.registers.toString());
    }
}
