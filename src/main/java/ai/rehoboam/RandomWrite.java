package ai.rehoboam;

import org.rocksdb.*;

import java.util.Random;

public class RandomWrite {

    static {
        RocksDB.loadLibrary();
    }
    private static int ttl=-1; // 2 hours
    private static FlushOptions fOptions;
    private static Options dbOptions;
    private static WriteOptions wOptions;
    private static BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
    private static final BloomFilter bloomFilter = new BloomFilter(8,false);
    private static Cache cache = new LRUCache(1024 * 1024, 8);//1GB
    private static RocksDB db=null;
    private static Random random = new Random();

    public static void main(String[] args) {
        String s = "dfsds";
        System.out.println(s.getBytes().length);
        init();
        String dbPath = args[1];
        String prefix = args[2];
        Integer totalKeys = Integer.valueOf(args[3]) * 1000;
        PayloadGen payloads = new PayloadGen();
        try {
            db = TtlDB.open(dbOptions, dbPath, ttl, false);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        if(args[0].compareTo("RandomWrite")==0){
            RandomWrite(prefix, totalKeys, payloads);
        }

        if(args[0].compareTo("CompactRange")==0){
            CompactRange();
        }
        if(args[0].compareTo("CountKeys")==0){
            CountKeys();
        }
    }

    private static void CompactRange() {
        Long time = System.currentTimeMillis();
        try {
            db.compactRange();
            System.out.println("getLatestSequenceNumber: " + db.getLatestSequenceNumber());
            System.out.println("Success in compaction, time taken: " + (System.currentTimeMillis()-time)/1000);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    private static void CountKeys() {
        RocksIterator rocksIterator = db.newIterator();
        rocksIterator.seekToFirst();
        long ct = 0;
        long size=0,ksize=0;
        while (rocksIterator.isValid()) {
            size+=rocksIterator.value().length;
            ksize+=rocksIterator.key().length;
            ct++;
            rocksIterator.next();
            if (ct % 100000 == 0)System.out.println("totalKeys in DB do far: " + ct);
        }
        System.out.println("totalKeys         : " + ct);
        System.out.println("totalSizeOfValues : " + size);
        System.out.println("totalSizeOfKeys   : " + ksize);
    }

    private static void RandomWrite(String prefix, Integer totalKeys, PayloadGen payloads) {
        Long time = System.currentTimeMillis();
        for (int i = 0; i < totalKeys; i++) {
            String skey = String.format("%s:%s%09d", prefix, payloads.getRandomString(30), random.nextInt(900000000));
            byte[] key = skey.getBytes();
            byte[] value = payloads.get1KBRandomPayload();

            try {
                db.put(wOptions,key, value);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
            if (i % 10000 == 0)
                System.out.println("Wrote " + i + " keys in " + ((System.currentTimeMillis() - time) / 1000) + "seconds. Total data:" + (i / 1000) + "MB");
        }
    }

    private static void init() {
        tableConfig.setBlockCache(cache);
        tableConfig.setBlockSize(256 * 1024); // 256KB
        tableConfig.setFilterPolicy(bloomFilter);
        //tableConfig.setBlockSizeDeviation(10);
        tableConfig.setBlockRestartInterval(1000); // => no keys within a block
        tableConfig.setIndexBlockRestartInterval(16); //
        tableConfig.setEnableIndexCompression(false);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        tableConfig.setPinTopLevelIndexAndFilter(true);
        tableConfig.setFormatVersion(5);
        tableConfig.setOptimizeFiltersForMemory(true);

        dbOptions = new Options().setCreateIfMissing(true);
        dbOptions.setMaxTotalWalSize(256 * 1024 * 1024L); //256MB
        dbOptions.setCompressionType(CompressionType.NO_COMPRESSION);
        dbOptions.setCompactionStyle(CompactionStyle.UNIVERSAL);
        dbOptions.setKeepLogFileNum(2);
        dbOptions.setAllowConcurrentMemtableWrite(true);
        dbOptions.setWriteBufferSize(256 * 1024 * 1024L); //32MB
        dbOptions.setMaxWriteBufferNumber(2);
        dbOptions.setMaxFileOpeningThreads(8);
        dbOptions.setCompactionReadaheadSize(4 * 1024 * 1024L); // 2MB
        dbOptions.setNewTableReaderForCompactionInputs(true);
        dbOptions.setTableFormatConfig(tableConfig);
        dbOptions.setOptimizeFiltersForHits(true);
        dbOptions.setSkipStatsUpdateOnDbOpen(true);
        //dbOptions.setNewTableReaderForCompactionInputs(false);
        dbOptions.setUnorderedWrite(true);
        dbOptions.setUseDirectIoForFlushAndCompaction(false);

        Env dbOptionsEnv = dbOptions.getEnv();
        dbOptionsEnv.setBackgroundThreads(4,Priority.LOW);
        dbOptionsEnv.setBackgroundThreads(12,Priority.HIGH);

        fOptions = new FlushOptions();
        fOptions.setWaitForFlush(true);

        wOptions = new WriteOptions();
        wOptions.setDisableWAL(true);
        wOptions.setSync(false);
        wOptions.setNoSlowdown(true);
    }
}
