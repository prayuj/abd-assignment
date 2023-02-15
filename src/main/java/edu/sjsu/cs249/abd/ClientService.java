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
    private static final Logger logger = Logger.getLogger(ClientService.class.getName());
    public void asyncWriteToRegister(long register, long value, String[] serverList, Level logLevel){
        logger.setLevel(logLevel);
        final CountDownLatch finishLatch = new CountDownLatch(serverList.length / 2 + 1);
        var writeRequest =Grpc.WriteRequest.newBuilder()
                .setAddr(register)
                .setLabel(System.currentTimeMillis())
                .setValue(value)
                .build();
        for (String server: serverList) {
            logger.log(Level.CONFIG, server + ": Going to write `"+value+"` to "+register);
            logger.log(Level.CONFIG, server + ": write request. Will send: " +
                    "\n -> register = " + writeRequest.getAddr() +
                    "\n -> label = " + writeRequest.getLabel() +
                    "\n -> value = " + writeRequest.getValue());
            var channel = this.createChannel(server);
            var stub = ABDServiceGrpc.newStub(channel);
            StreamObserver<WriteResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(WriteResponse value) {
                }
                @Override
                public void onError(Throwable cause) {
                    channel.shutdownNow();
                    logger.log(Level.WARNING, server + ": Error occurred while writing, cause " + cause.getMessage());
                    System.out.println("failure");
                }
                @Override
                public void onCompleted() {
                    channel.shutdownNow();
                    logger.log(Level.CONFIG, server + ": Stream completed");
                    finishLatch.countDown();
                }
            };
            stub.write(writeRequest, responseObserver);
        }
        try {
            if (!(finishLatch.await(3, TimeUnit.SECONDS)))
                logger.log(Level.SEVERE, "write request timeout to some/all servers!");
                System.out.println("failure");
        } catch (Exception ex) {
            logger.log(Level.SEVERE, ex.getMessage());
            System.out.println("failure");
        }
        if (finishLatch.getCount()!=0) {
            logger.log(Level.SEVERE, "write request did not write to a majority!");
            System.out.println("failure");
            return;
        }
        logger.log(Level.INFO, "write request succeeded!");
        System.out.println("success");
    }
    public void asyncReadFromRegister(long register, String[] serverList, Level logLevel) {
        logger.setLevel(logLevel);
        final CountDownLatch read1Latch = new CountDownLatch(serverList.length / 2 + 1);
        List<long[]> values = new ArrayList<>();
        var read1Request = Grpc.Read1Request.newBuilder()
                .setAddr(register)
                .build();
        for (String server: serverList) {
            logger.log(Level.CONFIG, server + ": Going to read register "+register);
            var channel = this.createChannel(server);
            var stub = ABDServiceGrpc.newStub(channel);
            StreamObserver<Read1Response> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(Read1Response value) {
                    if (value.getRc() == 0) {
                        logger.log(Level.CONFIG, server + ": read 1 response received. Received: " +
                                "\n -> value = " + value.getValue() +
                                "\n -> label = " + value.getLabel() +
                                "\n -> rc = " + value.getRc());
                        values.add(new long[]{value.getLabel(), value.getValue()});
                    } else {
                        logger.log(Level.CONFIG, server + ": read 1 response received. Received: " +
                                "\n -> rc = " + value.getRc());
                    }
                    read1Latch.countDown();
                }
                @Override
                public void onError(Throwable cause) {
                    logger.log(Level.WARNING, server + ": Error occurred while reading, cause " + cause.getMessage());
                    channel.shutdownNow();
                }
                @Override
                public void onCompleted() {
                    logger.log(Level.CONFIG, server + ": Stream completed");
                    channel.shutdownNow();
                }
            };
            stub.read1(read1Request, responseObserver);
        }
        try {
            if (!(read1Latch.await(3, TimeUnit.SECONDS))) {
                logger.log(Level.SEVERE, "read1 request timeout to some/all servers!");
                System.out.println("failed");
                return;
            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, ex.getMessage());
        }
        if (values.isEmpty()) {
            logger.log(Level.SEVERE, "read1 request did not return any values!");
            System.out.println("failed");
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

        logger.log(Level.INFO, "read1 complete, value: " + bestValue + "("+ maximumLabel +")");

        var read2Request = Grpc.Read2Request.newBuilder()
                .setAddr(register)
                .setLabel(maximumLabel)
                .setValue(bestValue)
                .build();
        final CountDownLatch read2Latch = new CountDownLatch(serverList.length / 2 + 1);
        for (String server: serverList) {
            logger.log(Level.CONFIG, server + ": Going to perform read2 on register "+register);
            var channel = this.createChannel(server);
            var stub = ABDServiceGrpc.newStub(channel);
            StreamObserver<Read2Response> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(Read2Response value) {
                }

                @Override
                public void onError(Throwable cause) {
                    logger.log(Level.WARNING, server + ": Error occurred while reading, cause " + cause.getMessage());
                    System.out.println("failed");
                    channel.shutdownNow();
                }

                @Override
                public void onCompleted() {
                    logger.log(Level.CONFIG, server + ": Stream completed");
                    read2Latch.countDown();
                    channel.shutdownNow();
                }
            };
            stub.read2(read2Request, responseObserver);
        }
        try {
            if (!(read2Latch.await(3, TimeUnit.SECONDS))) {
                logger.log(Level.SEVERE, "read2 request timeout to some/all servers!");
                System.out.println("failed");
                return;
            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, ex.getMessage());
            System.out.println("failed");
            return;
        }
        if (read2Latch.getCount() != 0) {
            logger.log(Level.SEVERE, "read2 request did not write to a majority!");
            System.out.println("failed");
            return;
        }
        logger.log(Level.INFO, "read2 request succeeded!");
        System.out.println(bestValue + "("+ maximumLabel +")");
    }
    public void enableServers(boolean write, boolean read1, boolean read2, String[] serverList) {
        for (String server: serverList) {
            logger.log(Level.CONFIG, server + ": sending enable request.");
            var enableRequest = Grpc.EnableRequest.newBuilder()
                    .setWrite(write)
                    .setRead1(read1)
                    .setRead2(read2).build();
            var stub = ABDServiceGrpc.newBlockingStub(createChannel(server));
            stub.enableRequests(enableRequest);
        }
    }
    private io.grpc.ManagedChannel createChannel(String serverAddress){
        var lastColon = serverAddress.lastIndexOf(':');
        var host = serverAddress.substring(0, lastColon);
        var port = Integer.parseInt(serverAddress.substring(lastColon+1));
        return ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
    }



}
