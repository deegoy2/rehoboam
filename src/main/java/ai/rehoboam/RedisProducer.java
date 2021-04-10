package ai.rehoboam;

import com.walmart.cbb3.defs.Conflation.proto.ConflationEventProto;
import com.walmart.cbb3.defs.Conflation.proto.ConflationGraphProto;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Random;

public class RedisProducer {
    public static void main(String[] args) throws Exception {


        int d = -11;
        for(int i=0;i<23;i++){
            d++;
            System.out.println(d + "\t" + d%5);
        }


        int n = 100;
        Random random = new Random();
        byte[] someInputBytes = new byte[n];
        random.nextBytes(someInputBytes);
        byte[] inputBytes = someInputBytes.clone();
//        for (int i = 0; i < n; i++) { System.out.print((int) inputBytes[i] + " "); }System.out.println();

        String inputString = new String(someInputBytes, "ISO-8859-1");

        byte[] outputBytes = inputString.getBytes("ISO-8859-1");
//        for (int i = 0; i < n; i++) { System.out.print((int) outputBytes[i] + " "); }System.out.println();

        ConflationEventProto.ConflationEvent conflationEvent;
        ConflationGraphProto.ConflationGraph.Edge.Builder edgeBuilder = ConflationGraphProto.ConflationGraph.Edge.newBuilder();
        ConflationEventProto.ConflationEvent.Builder builder = ConflationEventProto.ConflationEvent.newBuilder();
        builder.setEventType(ConflationEventProto.ConflationEvent.EventType.Conflate);
        String key;
        String otherKey;
        byte[] value;

        Jedis jedis = new Jedis("127.0.0.1", 6379);
        int ct =-1;
        while(true){
            ct++;
            key = "tid"+ct;
            otherKey = "tid"+(ct+1);
            builder.setOtherTgid(otherKey);
            builder.setTimestamp(System.currentTimeMillis());
            builder.setEdge(edgeBuilder.clear().setNode1(key).setNode2(otherKey).setContext("manual").setFrequency(1).setWeight(1D));
            conflationEvent = builder.build();
            value = conflationEvent.toByteArray();

            HashMap<String, String> map = new HashMap<>();
            map.put(key , new String(value, "ISO-8859-1"));
            jedis.xadd("conflation", null, map);
            System.out.print(key + "\t" + conflationEvent.toString());
            Thread.sleep(200);
//            break;
        }
    }
}
