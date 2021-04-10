package ai.rehoboam;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class HelloWorld {
    public static void main(String[] args) throws Exception{
        BufferedWriter bw = new BufferedWriter(new FileWriter("/Users/d0g00kj/inKeys.txt"));

        String s = "deepak1";
        byte[] sBytes = s.getBytes();
        System.out.println(new String(sBytes));
        for(int i=0;i<sBytes.length;i++){
            bw.write(String.valueOf(sBytes[i]));
            bw.write(" ");
        }
        bw.newLine();
        for(int i=0;i<sBytes.length;i++){
            bw.write(String.valueOf(sBytes[i]));
            bw.write(" ");
        }
        bw.newLine();
        bw.flush();
        bw.close();

        BufferedReader br = new BufferedReader(new FileReader("/Users/d0g00kj/inKeys.txt"));

        s = br.readLine();
        while(s!=null) {
            System.out.println("here:" + s);
            String[] intArray = s.split(" ");
            byte[] keyBytes = new byte[intArray.length];
            for (int i = 0; i < intArray.length; i++) {
                keyBytes[i] = Byte.parseByte(intArray[i]);
            }
            System.out.println(new String(keyBytes));
            s = br.readLine();
        }
        byte[] val = getBytes("000000003B9AAF923030303030303030");
        System.out.println(val);
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
    private static byte[] getBytes(String str) {
        byte[] val = new byte[str.length() / 2];
        for (int i = 0; i < val.length; i++) {
            int index = i * 2;
            int j = Integer.parseInt(str.substring(index, index + 2), 16);
            val[i] = (byte) j;
        }
        return val;
    }
}
