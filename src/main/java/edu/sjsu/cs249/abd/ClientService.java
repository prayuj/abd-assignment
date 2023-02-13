package edu.sjsu.cs249.abd;

import io.grpc.ManagedChannelBuilder;
import edu.sjsu.cs249.abd.ABDServiceGrpc;
import edu.sjsu.cs249.abd.Grpc.*;
import edu.sjsu.cs249.abd.Grpc;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientService {
    private static Logger logger = Logger.getLogger(ClientService.class.getName());

    public void asyncWriteToRegister(long register, long value, String[] serverList) throws InterruptedException {
        final CountDownLatch finishLatch = new CountDownLatch(serverList.length / 2 + 1);
        var writeRequest =Grpc.WriteRequest.newBuilder()
                .setAddr(register)
                .setLabel(System.currentTimeMillis())
                .setValue(value)
                .build();
        for (String server: serverList) {
            logger.log(Level.INFO, server + ": Going to write `"+value+"` to "+register);
            var channel = this.createChannel(server);
            var stub = ABDServiceGrpc.newStub(channel);
            StreamObserver<WriteResponse> responseObserver = new StreamObserver<WriteResponse>() {
                @Override
                public void onNext(WriteResponse value) {
                }
                @Override
                public void onError(Throwable cause) {
                    channel.shutdownNow();
                    logger.log(Level.WARNING, server + ": Error occurred while writing, cause " + cause.getMessage());
                }
                @Override
                public void onCompleted() {
                    channel.shutdownNow();
                    logger.log(Level.INFO, server + ": Stream completed");
                    finishLatch.countDown();
                }
            };
            stub.write(writeRequest, responseObserver);
        }
        try {
            if (!(finishLatch.await(3, TimeUnit.SECONDS)))
                logger.log(Level.SEVERE, "write request timeout to some/all servers!");
        } catch (Exception ex) {
            logger.log(Level.SEVERE, ex.getMessage());
        }
        finishLatch.await(3, TimeUnit.SECONDS);
        if (finishLatch.getCount()!=0) {
            logger.log(Level.SEVERE, "WRITE request did not write to a majority!");
        }
    }
    public void asyncReadFromRegister(long register, String[] serverList) throws Exception {
        final CountDownLatch read1Latch = new CountDownLatch(serverList.length / 2 + 1);
        List<long[]> values = new ArrayList<>();
        for (String server: serverList) {
            logger.log(Level.INFO, server + ": Going to read register `"+register);
            var read1Request = Grpc.Read1Request.newBuilder()
                    .setAddr(register)
                    .build();
            var channel = this.createChannel(server);
            var stub = ABDServiceGrpc.newStub(channel);
            StreamObserver<Read1Response> responseObserver = new StreamObserver<Read1Response>() {
                @Override
                public void onNext(Read1Response value) {
                    logger.log(Level.INFO, server + ": Received: `"+value);
                    values.add(new long[]{value.getLabel(), value.getValue()});
                }
                @Override
                public void onError(Throwable cause) {
                    channel.shutdownNow();
                    logger.log(Level.WARNING, server + ": Error occurred while reading, cause " + cause.getMessage());
                }
                @Override
                public void onCompleted() {
                    channel.shutdownNow();
                    logger.log(Level.INFO, server + ": Stream completed");
                    read1Latch.countDown();
                }
            };
            stub.read1(read1Request, responseObserver);
        }
        try {
            if (!(read1Latch.await(3, TimeUnit.SECONDS)))
                logger.log(Level.SEVERE, "read1 request timeout to some/all servers!");
        } catch (Exception ex) {
            logger.log(Level.SEVERE, ex.getMessage());
        }
        if (read1Latch.getCount() != 0) {
            logger.log(Level.SEVERE, "READ request did not write to a majority during r1!");
            return;
        }

        long maximumLabel = Integer.MIN_VALUE;
        long bestValue = 0;
        for (long[] value: values) {
            if (value[0] > maximumLabel) {
                maximumLabel = value[0];
                bestValue = value[1];
            }
        }

        final CountDownLatch read2Latch = new CountDownLatch(serverList.length / 2 + 1);
        for (String server: serverList) {
            logger.log(Level.INFO, server + ": Going to perform read2 on register `"+register);
            var read2Request = Grpc.Read2Request.newBuilder()
                    .setAddr(register)
                    .setLabel(maximumLabel)
                    .setValue(bestValue)
                    .build();
            var channel = this.createChannel(server);
            var stub = ABDServiceGrpc.newStub(channel);
            StreamObserver<Read2Response> responseObserver = new StreamObserver<Read2Response>() {
                @Override
                public void onNext(Read2Response value) {
                }

                @Override
                public void onError(Throwable cause) {
                    channel.shutdownNow();
                    System.out.println("Error occurred for server "+ server +", cause " + cause.getMessage());
                }

                @Override
                public void onCompleted() {
                    channel.shutdownNow();
                    logger.log(Level.INFO, server + ": Stream completed");
                    read2Latch.countDown();
                }
            };
            stub.read2(read2Request, responseObserver);
        }
        try {
            if (!(read2Latch.await(3, TimeUnit.SECONDS)))
                logger.log(Level.SEVERE, "read2 request timeout to some/all servers!");
        } catch (Exception ex) {
            logger.log(Level.SEVERE, ex.getMessage());
        }
        // TODO: do servers who have latest values return ack?
//        if (read2Latch.getCount() != 0) {
//            logger.log(Level.SEVERE, "READ request did not write to a majority during r2!");
//        }

    }
    private io.grpc.ManagedChannel createChannel(String serverAddress){
        var lastColon = serverAddress.lastIndexOf(':');
        var host = serverAddress.substring(0, lastColon);
        var port = Integer.parseInt(serverAddress.substring(lastColon+1));
        var channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
        return channel;
    }



}
