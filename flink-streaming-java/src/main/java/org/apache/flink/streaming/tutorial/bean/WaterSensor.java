package org.apache.flink.streaming.tutorial.bean;

import java.util.Objects;

/*
1. 类是公有的
2. 有无参构造器
3. 所有属性的类型都是可序列化的
 */
public class WaterSensor {
    public String id;
    public long ts;
    public Integer vc;

    // 务必要有空参构造器
    public WaterSensor() {
    }

    public WaterSensor(String id, long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor that = (WaterSensor) o;
        return getTs() == that.getTs() && Objects.equals(getId(), that.getId()) && Objects.equals(
                getVc(),
                that.getVc());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getTs(), getVc());
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }
}
