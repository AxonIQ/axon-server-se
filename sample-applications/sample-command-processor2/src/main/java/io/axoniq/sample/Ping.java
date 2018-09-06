package io.axoniq.sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Author: marc
 */
public class Ping {
    private static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumer;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                                                                  .forEach(consumer);
            System.out.println("Done");
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String hostName = args.length > 0 ? args[0] : "download.axoniq.io";
        boolean isWindows = System.getProperty("os.name")
                                  .toLowerCase().startsWith("windows");
        ProcessBuilder builder = new ProcessBuilder();
        String windowsPattern = "Reply from (.*) time=([0-9]+)ms TTL=([0-9]+)";
        String linuxPattern = "[0-9]+ bytes from (.*): icmp_seq=1 ttl=([0-9]+) time=([0-9\\.]+) ms";
        if (isWindows) {
            builder.command("ping", "-n", "1", hostName);
        } else {
            builder.command("ping", "-c", "1", hostName);
        }
        Process process = builder.start();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            reader.lines().forEach(line -> processLine(line, isWindows ? windowsPattern : linuxPattern));
        }
//        StreamGobbler streamGobbler =
//                new StreamGobbler(process.getInputStream(), Ping::processLine);
//        Executors.newSingleThreadExecutor().submit(streamGobbler);
        int exitCode = process.waitFor();
        System.out.println(exitCode + " - " + process.isAlive());
//        System.exit(exitCode);
    }

    private static void processLine(String text, String patternString) {
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(text);
        if( matcher.matches()) {
//            System.out.println(matcher.group(0));
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
            System.out.println(matcher.group(3));
        }

    }
}
