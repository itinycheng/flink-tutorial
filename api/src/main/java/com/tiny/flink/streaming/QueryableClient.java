package com.tiny.flink.streaming;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;

/**
 * queryable client
 * used to query flink states by key
 * <p>
 * it is very important to keep type consistent
 * means: if server use scala type, client must use same type; so that client implement language must use scala-lang
 *
 * @author tiny.wang
 */
public class QueryableClient {

    public static void main(String[] args) {
        try {
            QueryableStateClient cli = new QueryableStateClient(InetAddress.getByName("127.0.0.1"), 9069);
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("sum",
                    TypeInformation.of(new TypeHint<Integer>() {
                    }));

            CompletableFuture<ValueState<Integer>> future =
                    // NOTE - TINY: jobId is temporary
                    cli.getKvState(JobID.fromHexString("9c57432bb8dd64b24be147f25b5ff6ed"),
                            "word_count",
                            "QW"
                            , BasicTypeInfo.STRING_TYPE_INFO,
                            descriptor);

            ValueState<Integer> value = future.get();
            System.out.println(value.value());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
