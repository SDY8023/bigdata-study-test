package com.atguigu.beans;

/**
 * @ClassName SenorReading
 * @Description 传感器温度度数类
 * @Author SDY
 * @Date 2021/12/8 21:36
 **/
public class SenorReading {
    // 属性id,时间戳,温度,
    private String id;
    private Long timestamp;
    private Double temperature;

    public SenorReading() {
    }

    public SenorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SenorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
