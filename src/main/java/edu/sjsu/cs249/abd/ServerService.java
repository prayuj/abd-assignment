package edu.sjsu.cs249.abd;
import edu.sjsu.cs249.abd.Grpc.*;
import io.grpc.stub.StreamObserver;

import javax.naming.Name;
import java.util.HashMap;

public class ServerService extends edu.sjsu.cs249.abd.ABDServiceGrpc.ABDServiceImplBase{
    HashMap<Long,HashMap<String,Long>> registers;
    boolean write = true;
    boolean read1 = true;
    boolean read2 = true;

    ServerService() {
        registers = new HashMap<>();
    }
    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> responseObserver){
        if (!this.write) return;
        HashMap<String,Long> data = new HashMap<>();
        data.put("value", request.getValue());
        data.put("label", request.getLabel());
        this.registers.put(request.getAddr(),data);
        printRegisterValues();
        responseObserver.onNext(WriteResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
    @Override
    public void read1(Read1Request request, StreamObserver<Read1Response> responseObserver) {
        if (!this.read1) return;
        long registerAddress = request.getAddr();
        if(!this.registers.containsKey(registerAddress)) {
            responseObserver.onNext(Read1Response.newBuilder()
                    .setRc(1)
                    .build());
            responseObserver.onCompleted();

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
    @Override
    public void read2(Read2Request request, StreamObserver<Read2Response> responseObserver) {
        if (!this.read2) return;
        long registerAddress = request.getAddr();
        long label = request.getLabel();
        long value = request.getValue();
        HashMap<String,Long> data = new HashMap<>();
        if (!this.registers.containsKey(registerAddress)) {
            data.put("value", request.getValue());
            data.put("label", request.getLabel());
            this.registers.put(request.getAddr(),data);
            printRegisterValues();
        } else {
            HashMap<String,Long> storedData = this.registers.get(registerAddress);
            long storedDataLabel = storedData.get("label");
            if (storedDataLabel < label) {
                data.put("value", value);
                data.put("label", label);
                this.registers.put(registerAddress,data);
                printRegisterValues();
            }
        }
        responseObserver.onNext(Read2Response.newBuilder().build());
        responseObserver.onCompleted();
    }
    @Override
    public void name(NameRequest request, StreamObserver<NameResponse> responseObserver){
        responseObserver.onNext(NameResponse.newBuilder().
                    setName("Prayuj").build());
        responseObserver.onCompleted();
    }
    @Override
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver){
        System.exit(0);
    }

    @Override
    public void enableRequests(EnableRequest request, StreamObserver<EnableResponse> responseObserver) {
        this.write = request.getWrite();
        this.read1 = request.getRead1();
        this.read2 = request.getRead2();
        System.out.println("Enable Request, current status of operations:");
        System.out.println("write: " + write);
        System.out.println("read1: " + read1);
        System.out.println("read2: " + read2);
        responseObserver.onNext(EnableResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    public void delayFor(int seconds) {
        long now = System.currentTimeMillis();
        while (System.currentTimeMillis() - now < seconds * 1000);
    }

    private void printRegisterValues(){
        System.out.println("Current register values: " + this.registers.toString());
    }
}
