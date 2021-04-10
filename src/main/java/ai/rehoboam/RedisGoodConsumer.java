package ai.rehoboam;

import com.walmart.cbb3.defs.Conflation.ConflationEdge;
import com.walmart.cbb3.defs.Conflation.ConflationEventWrapper;
import com.walmart.cbb3.defs.Conflation.ConflationMetadataWrapper;
import com.walmart.cbb3.defs.Conflation.Serdes.ConflationEventWrapperDeserializer;
import com.walmart.cbb3.defs.Conflation.Utils.ConflationUtils;
import com.walmart.cbb3.defs.Conflation.proto.ConflationEventProto;
import com.walmart.cbb3.defs.Conflation.proto.ConflationGraphProto;
import com.walmart.cbb3.defs.Conflation.proto.ConflationMetadataProto;
import com.walmart.cbb3.defs.Conflation.proto.StoreDataProto;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

import java.io.UnsupportedEncodingException;
import java.util.*;

public class RedisGoodConsumer {

    public static ConflationEventProto.ConflationEvent.Builder eventBuilder;
    public static StoreDataProto.StoreData.Builder storeDataBuilder;
    public static ConflationGraphProto.ConflationGraph.Builder conflationGraphBuilder;
    public static ConflationGraphProto.ConflationGraph.Edge.Builder edgeBuilder;
    private static Jedis jedis;


    public static void main(String[] args) throws Exception {
        eventBuilder = ConflationEventProto.ConflationEvent.newBuilder();
        storeDataBuilder = StoreDataProto.StoreData.newBuilder();
        conflationGraphBuilder = ConflationGraphProto.ConflationGraph.newBuilder();
        edgeBuilder = ConflationGraphProto.ConflationGraph.Edge.newBuilder();
        jedis = new Jedis("127.0.0.1", 6379);
        StreamEntryID lastSeen = new StreamEntryID();
        ConflationEventWrapperDeserializer conflationEventWrapperDeserializer = new ConflationEventWrapperDeserializer();
        while(true) {
            Map.Entry<String, StreamEntryID> streamQeury = new AbstractMap.SimpleImmutableEntry<>("conflation", lastSeen);
            List<Map.Entry<String, List<StreamEntry>>> range = jedis.xread(1, 2000L, streamQeury);
            if (range.size() == 0) {
                System.out.println("No events read in 2000ms");
                continue;
            }
            List<StreamEntry> streamEntries = range.iterator().next().getValue();
            for (StreamEntry streamEntry: streamEntries) {
                StreamEntryID id = streamEntry.getID();
                lastSeen = id;
                Map<String, String> fields = streamEntry.getFields();
                Map.Entry<String, String> stringEntry = fields.entrySet().iterator().next();

                byte[] valueBytes= stringEntry.getValue().getBytes("ISO-8859-1");

                ConflationEventWrapper cew = conflationEventWrapperDeserializer.deserialize("", valueBytes);
                if(cew==null) continue;
                process(stringEntry.getKey(),cew);

//                System.out.print(stringEntry.getKey() + "\t");
//                for(int j=0;j<valueBytes.length;j++){ System.out.print(valueBytes[j] + " "); }System.out.println();
            }
        }
    }

    public static void forward(String key, ConflationEventProto.ConflationEvent cew){
        HashMap<String, String> map = new HashMap<>();
        try {
            map.put(key , new String(cew.toByteArray(), "ISO-8859-1"));
            jedis.xadd("conflation", null, map);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public static void process(String key, ConflationEventWrapper cew) {
        String value = jedis.get(key);
        ConflationMetadataWrapper cmw = getConflationMetadataWrapper(key, value);
        if(cmw==null) return;

        if (cew.getEventType() == ConflationEventProto.ConflationEvent.EventType.ChangeRoot) {
            handleChangeRootEvent(key, cmw, cew);
        } else if (cmw.getState() == ConflationMetadataProto.ConflationMetadata.State.NORMAL && !key.equals(cmw.getTgid())) {
            forward(cmw.getTgid(), cew.getConflationEvent());
            System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Routing event to parent: " + cmw.getTgid());
        } else if (cew.getEventType() == ConflationEventProto.ConflationEvent.EventType.Conflate) {
            if (cmw.getState() == ConflationMetadataProto.ConflationMetadata.State.PACKED) {
                forward(cmw.getOtherTgid(), cew.getConflationEvent());
                System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Routing Conflate event to parent (PACKED): " + cmw.getOtherTgid());
            } else if (cmw.getConflatedTids().contains(cew.getOtherTgid())) {
                System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Already conflated " + cew.getOtherTgid());
                cmw.getConflationGraph().addOrUpdateEdge(cew.getConflationEdge());
                putConflationMetadata(key, cmw);
            } else if (cmw.getConflatedTids().size() >= 1000) {
                System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Swallowing Conflation event as current tgid already has the defined limit of tids : " + 1000);
            } else {
                initiateConflationSequence(key, cmw, cew);
                System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Initiating New Conflation");
            }
        } else if (cew.getEventType() == ConflationEventProto.ConflationEvent.EventType.SendRoot) {

            if (key.equals(cew.getOtherTgid())) {
                if(cmw.getState() == ConflationMetadataProto.ConflationMetadata.State.PACKED){
                    System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Routing SR to parent (PACKED): " + cmw.getOtherTgid());
                    forward(cmw.getOtherTgid(), cew.getConflationEvent());
                }
                else {
                    cmw.getConflationGraph().addOrUpdateEdge(cew.getConflationEdge());
                    putConflationMetadata(key, cmw);
                    System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Received SendRoot on origin");
                }
                return;
            }

            // Detecting over-conflations
            if (cmw.getConflatedTids().size() + cew.getConflatedTids().size() > 1000) {
                System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Swallowing SendRoot event as this conflation would exceed the threshold on the number of TIDs in a set: " + 1000);
                return;
            }

            // SendRoot(tid0) event is received on tid2
            if (ConflationUtils.compare(key, cew.getOtherTgid()) > 0) {
                if (cmw.getState() == ConflationMetadataProto.ConflationMetadata.State.PACKED) {
                    // SendRoot event originating from a tgid contains all the conflated TIDs of that tgid in the ConflationSet
                    // if this set contains the tid that this tid is PACKED to, it implies  event is being re-processed and that data needs to be sent again.
                    if (cew.getOtherTgid().equals(cmw.getOtherTgid()) && cmw.getTimestamp()==cew.getTimestamp()) {
                        // reprocessing SendRoot, sending data again
                        System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Sending data (again) to " + cew.getOtherTgid());
                        sendSendDataEvent(key, cew, cmw);
                    } else {
                        // tid2 receives SendRoot(tid0) event implying tid0 wants to conflate with tid2 but tid2 is already packed to tid1
                        // so tid2 forwards incoming event to tid1
                        System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Routing SR to parent (PACKED): " + cmw.getOtherTgid());
                        forward(cmw.getOtherTgid(), cew.getConflationEvent());
                    }
                } else {
                    // SendData to root as otherTgid is the final-tgid
                    sendSendDataEvent(key, cew, cmw);
                    System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Sending data to " + cew.getOtherTgid());
                }
            }
            // SendRoot(tid2) event is received on tid1
            else if (ConflationUtils.compare(key, cew.getOtherTgid()) < 0) {
                if (cmw.getState() == ConflationMetadataProto.ConflationMetadata.State.PACKED) {
                    // tid1 receives SendRoot(tid2) event implying tid2 wants to conflate with tid1 but tid1 is already packed to tid0
                    System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Routing SR to parent (PACKED): " + cmw.getOtherTgid());
                    forward(cmw.getOtherTgid(), cew.getConflationEvent());
                } else {
                    if (cmw.getConflatedTids().containsAll(cew.getConflatedTids())) {
                        // Since conflation is already done with tid2 and its children, send ChangeRoot events to tid2 and its children
                        cmw.getConflationGraph().addOrUpdateEdge(cew.getConflationEdge());
                        putConflationMetadata(key, cmw);
                        System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: No-op on SendRoot");
                        return;
                    }
                    // send back this.root to other tgid as this.root < sendRoot.root, so this is the final tgid
                    initiateConflationSequence(key, cmw, cew);
                    System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: send back SendRoot");
                }
            }
        } else if (cew.getEventType() == ConflationEventProto.ConflationEvent.EventType.SendData) {
            if (cmw.getState() == ConflationMetadataProto.ConflationMetadata.State.PACKED) {
                System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Routing SendData to parent (PACKED): " + cmw.getOtherTgid());
                forward(cmw.getOtherTgid(), cew.getConflationEvent());
            } else {
                // data already present
                if (cmw.getConflatedTids().contains(cew.getOtherTgid())) {
                    System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Re-Consuming data from SendDataEvent " + cew.getOtherTgid());
                } else {
                    System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Consuming data from SendDataEvent " + cew.getOtherTgid());
                }
                handleSendDataEvent(key, cew, cmw);
                if(cmw.getConflatedTids().size()>1000) System.out.println("BigKey: " + key); // Debug log
                postConflation(cmw.getTgid(), new HashSet<>(Arrays.asList(cew.getOtherTgid())));
            }
        }
    }

    private static ConflationMetadataWrapper getConflationMetadataWrapper(String key, String value) {
        if (value == null) {
            // conflation metadata == null => tid is its own tgid
            ConflationMetadataWrapper cmw = new ConflationMetadataWrapper(key);
            cmw.addToConflatedTids(key);
            return cmw;
        }
        ConflationMetadataWrapper conflationMetadataWrapper = new ConflationMetadataWrapper(key);
        try {
            byte[] bytes = value.getBytes("ISO-8859-1");
            conflationMetadataWrapper = new ConflationMetadataWrapper(bytes);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
        return conflationMetadataWrapper;
    }

    private static void sendSendDataEvent(String key, ConflationEventWrapper cew, ConflationMetadataWrapper cmw) {
        Map<String, List<StoreDataProto.StoreData>> storeDataMap = buildStoreDataMap(key, cmw);
// Uncomment for multi SendData data approach
//        for (String conflatedTid : cmw.getConflatedTids()) {
//            if (conflatedTid.equals(key)) continue;
//            List<StoreDataProto.StoreData> storeDataList = storeDataMap.getOrDefault(conflatedTid, new ArrayList<>());
//            ConflationEventProto.ConflationEvent sendDataEvent = eventBuilder.clear()
//                    .setEventType(ConflationEventProto.ConflationEvent.EventType.SendData)
//                    .setOtherTgid(conflatedTid)
//                    .addAllStoresData(storeDataList)
//                    .setTimestamp(cew.getTimestamp())
//                    .build();
//            forward(cew.getOtherTgid(), sendDataEvent);
//        }

        conflationGraphBuilder.clear();
        for (ConflationEdge conflationEdge : cmw.getConflationGraph().edges()) {
            conflationGraphBuilder.addEdges(edgeBuilder.clear()
                    .setNode1(conflationEdge.getNode1())
                    .setNode2(conflationEdge.getNode2())
                    .setContext(conflationEdge.getContext())
                    .setFrequency(conflationEdge.getFrequency())
                    .setWeight(conflationEdge.getWeight()).build()
            );
        }
        List<StoreDataProto.StoreData> storeDataList = storeDataMap.getOrDefault(key, new ArrayList<>());
        ConflationEventProto.ConflationEvent sendDataEvent = eventBuilder.clear()
                .setEventType(ConflationEventProto.ConflationEvent.EventType.SendData)
                .setOtherTgid(key)
                .addAllStoresData(storeDataList)
                .addAllConflatedTid(cmw.getConflatedTids()) // Comment for multi SendData data approach
                .setEdge(edgeBuilder.clear()
                        .setNode1(cew.getConflationEdge().getNode1())
                        .setNode2(cew.getConflationEdge().getNode2())
                        .setContext(cew.getConflationEdge().getContext())
                        .setFrequency(cew.getConflationEdge().getFrequency())
                        .setWeight(cew.getConflationEdge().getWeight()).build())
                .setConflationGraph(conflationGraphBuilder)
                .setTimestamp(cew.getTimestamp())
                .build();
        forward(cew.getOtherTgid(), sendDataEvent);

        cmw.setState(ConflationMetadataProto.ConflationMetadata.State.PACKED);
        cmw.setOtherTgid(cew.getOtherTgid());
        cmw.setTimestamp(cew.getTimestamp());
        putConflationMetadata(key, cmw);
    }

    private static void initiateConflationSequence(String key, ConflationMetadataWrapper cmw, ConflationEventWrapper cew) {
        // initiate new conflation by triggering a SendRoot event
        ConflationEventProto.ConflationEvent sendRootEvent = eventBuilder.clear()
                .setEventType(ConflationEventProto.ConflationEvent.EventType.SendRoot)
                .addAllConflatedTid(cmw.getConflatedTids())
                .setOtherTgid(key)
                .setTimestamp(cew.getTimestamp())
                .setEdge(ConflationGraphProto.ConflationGraph.Edge.newBuilder()
                        .setNode1(cew.getConflationEdge().getNode1())
                        .setNode2(cew.getConflationEdge().getNode2())
                        .setWeight(cew.getConflationEdge().getWeight())
                        .setContext(cew.getConflationEdge().getContext())
                        .setFrequency(cew.getConflationEdge().getFrequency()).build())
                .build();
        forward(cew.getOtherTgid(), sendRootEvent);
        putConflationMetadata(key, cmw);
    }

    private static void sendChangeRoot(String key, ConflationEventWrapper cew) {
        ConflationEventProto.ConflationEvent changeRoot = eventBuilder.clear()
                .setEventType(ConflationEventProto.ConflationEvent.EventType.ChangeRoot)
                .setOtherTgid(key)
                .setTimestamp(cew.getTimestamp())
                .build();
        //forward(cew.getOtherTgid(), changeRoot);
        // Uncomment above and comment this loop for multi SendData data approach
        for(String tid:cew.getConflatedTids()) {
            forward(tid, changeRoot);
        }
    }

    private static void handleSendDataEvent(String key, ConflationEventWrapper cew, ConflationMetadataWrapper cmw) {
        restoreStores(cew.getStoreDataList(), key);
        sendChangeRoot(key, cew);
        cmw.addToConflatedTids(cew.getOtherTgid());
        // Comment the following line of code for multi SendData data approach
        cmw.addToConflatedTids(cew.getConflatedTids());
        if (cew.hasEdge()) cmw.getConflationGraph().addOrUpdateEdge(cew.getConflationEdge());
        for (ConflationEdge conflationEdge : cew.getConflationEdges()) {
            cmw.getConflationGraph().addOrUpdateEdge(conflationEdge);
        }
        cmw.setOtherTgid(null);
        putConflationMetadata(key, cmw);
    }

    private static void handleChangeRootEvent(String key, ConflationMetadataWrapper cmw, ConflationEventWrapper cew) {
        deleteDataForTgid(key);
        if (ConflationUtils.compare(cmw.getTgid(), cew.getOtherTgid()) <= 0) {
            System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Received out-of-order ChangeRoot");
            return;
        }
        cmw = new ConflationMetadataWrapper(cew.getOtherTgid());
        putConflationMetadata(key, cmw);
        System.out.println(" Key: " + key + " Event: " + cew.toString() + " Outcome: Change Root to " + cew.getOtherTgid() + " for " + key);
    }

    private static void deleteDataForTgid(String tgid) {
//        for (Map.Entry<String, CBBStoreWrapper> cbbStoreWrapperEntry : getCBBStoreWrapperMap().entrySet()) {
//            if (cbbStoreWrapperEntry.getKey().equals(StoreName.LINKAGE_STORE.getName())) continue;
//            CBBStoreWrapper cbbStoreWrapper = cbbStoreWrapperEntry.getValue();
//            for (String columnFamily : cbbStoreWrapper.listColumnFamilies()) {
//                cbbStoreWrapper.deleteAllForTgid(columnFamily, Bytes.wrap(tgid.getBytes()));
//            }
//        }
//        for (String colFam : getEventStore().listColumnFamilies()) {
//            getEventStore().deleteAllForTgid(colFam, Bytes.wrap(tgid.getBytes()));
//        }
//        for (String colFam : getFacetStore().listColumnFamilies()) {
//            getFacetStore().deleteAllForTgid(colFam, Bytes.wrap(tgid.getBytes()));
//        }
    }

    private static void restoreStores(List<StoreDataProto.StoreData> storeDataList, String tgid) {
//        for (StoreDataProto.StoreData storeData : storeDataList) {
//            String tgidTid = tgid.concat(storeData.getKey());
//            byte[] val = storeData.getValue().toByteArray();
//            if (storeData.getStoreName().equals(StoreName.EVENT_STORE.getName())) {
//                getEventStore().put(storeData.getColumnFamily(), Bytes.wrap(tgidTid.getBytes(Charset.forName(Constants.UTF8))), val);
//            } else if (storeData.getStoreName().equals(StoreName.FACET_STORE.getName())) {
//                getFacetStore().put(storeData.getColumnFamily(), Bytes.wrap(tgidTid.getBytes(Charset.forName(Constants.UTF8))), val);
//            } else {
//                getCBBStoreWrapper(storeData.getStoreName()).put(storeData.getColumnFamily(), Bytes.wrap(tgidTid.getBytes(Charset.forName(Constants.UTF8))), val);
//            }
//        }
    }

    private static Map<String, List<StoreDataProto.StoreData>> buildStoreDataMap(String tgid, ConflationMetadataWrapper cmw) {
        HashMap<String, List<StoreDataProto.StoreData>> tidToStoreDataMap = new HashMap<>();
//        String storeName;
//        for (Map.Entry<String, CBBStoreWrapper> cbbStoreWrapperEntry : getCBBStoreWrapperMap().entrySet()) {
//            storeName = cbbStoreWrapperEntry.getKey();
//            if (storeName.equals(StoreName.LINKAGE_STORE.getName())) continue;
//            CBBStoreWrapper cbbStoreWrapper = cbbStoreWrapperEntry.getValue();
//            for (String columnFamily : cbbStoreWrapper.listColumnFamilies()) {
//                for (String tid : cmw.getConflatedTids()) {
//                    List<KeyValue<Bytes, byte[]>> keyValues = cbbStoreWrapper.getAllForKey(columnFamily, Bytes.wrap(tgid.concat(tid).getBytes()));
//                    fillStoreDataMap(storeName, columnFamily, tgid, tid, keyValues, tidToStoreDataMap);
//                }
//            }
//        }
//        EventStoreWrapper eventStoreWrapper = getEventStore();
//        storeName = StoreName.EVENT_STORE.getName();
//        for (String columnFamily : eventStoreWrapper.listColumnFamilies()) {
//            for (String tid : cmw.getConflatedTids()) {
//                String tgidTid = tgid.concat(tid);
//                List<KeyValue<Bytes, byte[]>> keyValues = eventStoreWrapper.getAllForKey(columnFamily, Bytes.wrap(tgidTid.getBytes()));
//                int eventLimit = CCMManager.getInstance().getAppConfig().getEventLimit();
//                keyValues = keyValues.subList(Math.max(keyValues.size() - eventLimit, 0), keyValues.size());
//                fillStoreDataMap(storeName, columnFamily, tgid, tid, keyValues, tidToStoreDataMap);
//            }
//        }
//        FacetStoreWrapper facetStoreWrapper = getFacetStore();
//        storeName = StoreName.FACET_STORE.getName();
//        for (String columnFamily : facetStoreWrapper.listColumnFamilies()) {
//            for (String tid : cmw.getConflatedTids()) {
//                String tgidTid = tgid.concat(tid);
//                List<KeyValue<Bytes, byte[]>> keyValues = facetStoreWrapper.getAllForKey(columnFamily, Bytes.wrap(tgidTid.getBytes()));
//                fillStoreDataMap(storeName, columnFamily, tgid, tid, keyValues, tidToStoreDataMap);
//            }
//        }
        return tidToStoreDataMap;
    }

//    private void fillStoreDataMap(String storeName, String columnFamily, String tgid, String tid, List<KeyValue<Bytes, byte[]>> keyValues, HashMap<String, List<StoreDataProto.StoreData>> storeDataMap) {
//        for (KeyValue<Bytes, byte[]> keyValue : keyValues) {
//            String key = new String(keyValue.key.get());
//            String keyWithoutTgid = key.substring(tgid.length());
//            ByteString valueBytes = ByteString.copyFrom(keyValue.value);
//            StoreDataProto.StoreData storeData = storeDataBuilder.clear()
//                    .setStoreName(storeName)
//                    .setColumnFamily(columnFamily)
//                    .setKey(keyWithoutTgid)
//                    .setValue(valueBytes)
//                    .build();
//            if (storeDataMap.get(tid) == null) storeDataMap.put(tid, new ArrayList<>());
//            storeDataMap.get(tid).add(storeData);
//        }
//    }

    private static void putConflationMetadata(String key, ConflationMetadataWrapper cmw){
        byte[] conflationMetadataBytes = cmw.serializeConflationMetadataWrapper();
        String value;
        try {
            value = new String(conflationMetadataBytes, "ISO-8859-1");
            jedis.set(key,value);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private static void postConflation(String tgid, HashSet<String> newTids) {

    }

}
