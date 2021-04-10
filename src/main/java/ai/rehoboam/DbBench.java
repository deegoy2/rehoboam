package ai.rehoboam;

import org.rocksdb.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Random;

public class DbBench {

    static {
        RocksDB.loadLibrary();
    }
    private static FlushOptions fOptions;
    private static Options dbOptions;
    private static WriteOptions wOptions;
    private static BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
    private static final BloomFilter bloomFilter = new BloomFilter(8,false);
    private static Cache cache = new LRUCache(1024 * 1024, 8);//1GB
    private static RocksDB db=null;
    private static Random random = new Random();

    public static void main(String[] args) throws Exception{
        init();
        String dbPath = args[1];


        if(args[0].compareTo("CountKeys")==0){
            try {
                db = RocksDB.openReadOnly(dbOptions, dbPath);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
            CountKeys();
        }
        if(args[0].compareTo("RangeRead")==0){
            try {
                db = RocksDB.openReadOnly(dbOptions, dbPath);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
            RangeRead(args);
        }
    }

    private static void CountKeys() throws Exception{
        RocksIterator rocksIterator = db.newIterator();
        rocksIterator.seekToFirst();
        long ct = 0;
        long size=0,ksize=0;
        BufferedWriter bw = new BufferedWriter(new FileWriter("inKeys.txt"));
        while (rocksIterator.isValid()) {
            size += rocksIterator.value().length;
            ksize += rocksIterator.key().length;
            ct++;
            String skey = new String(rocksIterator.key());
            String sval = new String(rocksIterator.value());
            System.out.print(skey + "\t" + sval + "\t");
            System.out.println("totalKeys in DB do far: " + ct);
            for (int i = 0; i < rocksIterator.key().length; i++) {
                System.out.print(String.valueOf(rocksIterator.key()[i]) + " ");
                bw.write(String.valueOf(rocksIterator.key()[i]));
                bw.write(" ");
            }
            System.out.println();
            bw.newLine();
            bw.flush();
            rocksIterator.next();
        }
        bw.close();
        System.out.println("totalKeys         : " + ct);
        System.out.println("totalSizeOfValues : " + size);
        System.out.println("totalSizeOfKeys   : " + ksize);
    }

    private static void RangeRead(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("inKeys.txt"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("result.txt"));
        byte[] from = getBytes(br.readLine());
        int limit = Integer.valueOf(args[2]);
        long ts = System.currentTimeMillis();
        RocksIterator iterator = db.newIterator();
        iterator.seek(from);
        int ct = 0;
        while (iterator.isValid() && ct < limit) {
            bw.write(bytesToHex(iterator.key()));
            bw.newLine();
            iterator.next();
            ct++;
        }
        System.out.println("totalKeys read: " + ct);
        System.out.println("time taken    : " + (System.currentTimeMillis()-ts));
        bw.flush();
        iterator.close();
    }

    private static byte[] getBytes(String str) {
        byte[] val = new byte[str.length() / 2];
        for (int i = 0; i < val.length; i++) {
            int index = i * 2;
            int j = Integer.parseInt(str.substring(index, index + 2), 16);
            val[i] = (byte) j;
        }
        return val;
    }
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
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
        dbOptions.compactionOptionsFIFO().setAllowCompaction(true);
        dbOptions.setMaxTotalWalSize(256 * 1024 * 1024L); //256MB
        dbOptions.setCompressionType(CompressionType.NO_COMPRESSION);
        dbOptions.setCompactionStyle(CompactionStyle.FIFO);
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
