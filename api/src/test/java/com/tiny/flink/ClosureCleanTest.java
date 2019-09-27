package com.tiny.flink;

import com.tiny.flink.entity.ClosureOuter;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * test-code for ClosureCleaner
 */
public class ClosureCleanTest {

    @Test
    public void clean() throws Exception {
        ClosureOuter outer = new ClosureOuter();
        ClosureOuter.FirstInner first = outer.new FirstInner();
        ClosureOuter.FirstInner.SecondInner second = first.new SecondInner();
        ClosureOuter.FirstInner.SecondInner.ThirdInner third = second.new ThirdInner();
        Field[] fields = first.getClass().getDeclaredFields();
        Field field = Arrays.stream(fields)
                .filter(f -> f.getName().startsWith("this$"))
                .findAny()
                .orElse(null);
        assert field != null;
        field.setAccessible(true);
        Object before = field.get(first);
        ClosureCleaner.clean(first, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
        Object after = field.get(first);
        Assert.assertNotEquals(before, after);
    }
}
