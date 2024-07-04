package com.icloud

import com.icloud.function.TemperatureAverager
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object AverageSensorReading {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.config.autoWatermarkInterval = 1_000L

        val sensorData = env
            .addSource(SensorSource())
            .assignTimestampsAndWatermarks(SensorTimeAssigner())

        val avgTemp =
            sensorData.map { it.copy(temperature = (it.temperature - 32) * (5.0 / 9.0)) }
                .global()
                .keyBy { it.id }
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .apply(TemperatureAverager())

        avgTemp.print("avg_temp")

        env.execute(("Compute average sensor temperature"))
    }
}