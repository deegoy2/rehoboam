package ai.rehoboam;

import java.util.Random;
public class PayloadGen {
    Random random = new Random();
    public byte[] get1KBRandomPayload(){
        byte[] arr = new byte[1024];
        random.nextBytes(arr);
        return arr;
    }
    public String getRandomString(int length){
        int leftLimit = 97;
        int rightLimit = 122;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomLimitedInt = leftLimit + (int)
                    (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        return buffer.toString();
    }
}
