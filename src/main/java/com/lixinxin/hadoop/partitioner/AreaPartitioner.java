package com.lixinxin.hadoop.partitioner;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Reducer 分组
 *
 * @param <K>
 * @param <V>
 */
public class AreaPartitioner<K, V> extends Partitioner<K, V> {
    private static HashMap<String, Integer> areaMap = new HashMap<String, Integer>();
    static {
        areaMap.put("135", 0);
        areaMap.put("136", 1);
        areaMap.put("137", 2);
        areaMap.put("138", 3);
        areaMap.put("139", 4);
    }
    @Override
    public int getPartition(K k, V v, int i) {
        //从key中拿到手机号，查询手机归属地字典，不同的省份返回不同的组号
        int areaCoder = areaMap.get(k.toString().substring(0, 3)) == null ? 5 : areaMap.get(k.toString().substring(0, 3));
        return areaCoder;
    }
}
