package edu.sjsu.cs249.abd;

import io.grpc.*;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import java.util.concurrent.Callable;



public class Main {

    public static void main(String[] args) {
        System.exit(new CommandLine(new Cli()).execute(args));
    }

    @Command(subcommands = {ServerCli.class, ClientCli.class})
    static class Cli {}

    @Command(name = "server", mixinStandardHelpOptions = true, description = "start an ABD register server.")
    static class ServerCli implements Callable<Integer> {
        @Parameters(index = "0", description = "port listen on.")
        int serverPort;
        @Override
        public Integer call() throws Exception {
            Server server = ServerBuilder.forPort(serverPort)
                    .addService(new ServerService())
                    .build();
            server.start();
            System.out.printf("will listen on port %s\n", server.getPort());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.shutdown();
                System.out.println("Successfully stopped the server");
            }));
            server.awaitTermination();
            return 0;
        }
    }

    @Command(name = "client", mixinStandardHelpOptions = true, description = "start and ADB client.")
    static class ClientCli {
        @Parameters(index = "0", description = "comma separated list of servers to use.")
        String servers;

        @Command
        public void read(@Parameters(paramLabel = "register") long register) throws Exception {
            String[] serversList = servers.split(",");
            new ClientService().asyncReadFromRegister(register, serversList);
        }

        @Command
        public void write(@Parameters(paramLabel = "register") long register,
                          @Parameters(paramLabel = "value") long value) throws InterruptedException {

            String[] serversList = servers.split(",");
            new ClientService().asyncWriteToRegister(register, value, serversList);

        }
    }
}
