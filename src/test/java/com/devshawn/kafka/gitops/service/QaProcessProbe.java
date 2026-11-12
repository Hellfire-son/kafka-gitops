package com.devshawn.kafka.gitops.service;

/** Local subprocess fixture. Never starts ccloud, Docker or a network client. */
public final class QaProcessProbe {
    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "ok" -> System.out.print("{\"id\":\"synthetic\"}");
            case "failed" -> { System.out.print("{\"id\":\"synthetic\"}"); System.err.print("private-process-error"); System.exit(7); }
            case "pending" -> Thread.sleep(20_000);
            case "oversize" -> System.out.write(new byte[1_048_577]);
            default -> throw new IllegalArgumentException();
        }
    }
}
