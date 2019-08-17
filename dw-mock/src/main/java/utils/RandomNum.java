package utils;

import java.util.Random;

public class RandomNum {

    /**
     * 获取指定范围内的随机整数
     * @param fromNum
     * @param toNum
     * @return
     */
    public static int getRandInt(int fromNum, int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}

