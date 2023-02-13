package edu.sjsu.cs249.abd;

import io.grpc.*;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import java.util.Objects;
import java.util.concurrent.Callable;



public class Main {

    public static void main(String[] args) {
        System.exit(new CommandLine(new Cli()).execute(args));
    }
    @Command(subcommands = {ServerCli.class, ClientCli.class, EnableRequestCli.class})
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

    @Command(name = "enableFlags", mixinStandardHelpOptions = true, description = "enable/disable servers.")
    static class EnableRequestCli implements Callable<Integer>{
        @Spec CommandSpec spec; // injected by picocli
        @Parameters(index = "0", description = "comma separated list of servers to use.")
        String servers;
        @Option(names = "--wf", description = "Flag for write requests", defaultValue = "enable") String write;
        @Option(names = "--r1f", description = "Flag for read1 requests", defaultValue = "enable") String read1;
        @Option(names = "--r2f", description = "Flag for read2 requests", defaultValue = "enable") String read2;
        @Override
        public Integer call() throws Exception {
            if (!(Objects.equals(write, "enable") || Objects.equals(write, "disable"))) throw new ParameterException(spec.commandLine(), "Invalid write enable option. Valid values: `enable` | `disable`");
            if (!(Objects.equals(read1, "enable") || Objects.equals(read1, "disable"))) throw new ParameterException(spec.commandLine(), "Invalid read1 enable option. Valid values: `enable` | `disable`");
            if (!(Objects.equals(read2, "enable") || Objects.equals(read2, "disable"))) throw new ParameterException(spec.commandLine(), "Invalid read2 enable option. Valid values: `enable` | `disable`");
            String[] serversList = servers.split(",");
            new ClientService().enableServers(write.equals("enable"), read1.equals("enable"), read2.equals("enable"), serversList);
            return 0;
        }
    }
}
