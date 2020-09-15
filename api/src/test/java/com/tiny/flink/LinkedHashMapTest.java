package com.tiny.flink;

import java.util.LinkedHashMap;
import java.util.Map;

public class LinkedHashMapTest {
    public static void main(String[] args) {
        Map<String, String> map = new LinkedHashMap<>(16, 0.75f, true);
        map.put("d", "d");
        map.put("a", "a");
        map.put("b", "b");
        map.put("c", "c");
        for (String value : map.values()) {
            System.out.println(value);
        }

        System.out.println("--------" + map.get("d"));

        for (String value : map.values()) {
            System.out.println(value);
        }
    }
}
