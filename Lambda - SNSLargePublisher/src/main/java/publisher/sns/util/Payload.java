package publisher.sns.util;

public class Payload {
    private static final String payload = "KaziTanvirAzad";

    public static String getPayload() {
        return payload.repeat(40000);
    }
}
