package com.icloud.function

import com.icloud.model.SensorReading
import com.icloud.extention.tuple
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class TemperatureAverager
    : WindowFunction<SensorReading, SensorReading, String, TimeWindow> {
    override fun apply(
        key: String,
        window: TimeWindow,
        input: MutableIterable<SensorReading>,
        out: Collector<SensorReading>,
    ) {
        val avgTemp = (0.0 tuple 0.0)
            .let { input.fold(it) { acc, sensorReading -> (acc.f0 + 1) tuple (acc.f1 + sensorReading.temperature) } }
            .let { it.f1 / it.f0 }

        out.collect(SensorReading(key, window.end, avgTemp))
    }

}