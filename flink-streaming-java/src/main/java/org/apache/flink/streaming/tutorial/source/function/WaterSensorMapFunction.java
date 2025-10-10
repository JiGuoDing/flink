package org.apache.flink.streaming.tutorial.source.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.tutorial.bean.WaterSensor;

public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        // 输入格式 id, ts, vc
        String[] fields = value.split(",");
        if (fields.length != 3) {
            throw new IllegalArgumentException("Incorrect number of fields");
        }

        String id = fields[0].trim();
        long ts = Long.parseLong(fields[1].trim());
        Integer vc = Integer.parseInt(fields[2].trim());

        return new WaterSensor(id, ts, vc);
    }
}
