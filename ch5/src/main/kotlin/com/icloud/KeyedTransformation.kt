package com.icloud

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner

object KeyedTransformation {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.config.autoWatermarkInterval = 1_000L

        val readings = env
            .addSource(SensorSource())
            .assignTimestampsAndWatermarks(SensorTimeAssigner())

        val keyed =
            readings.keyBy { it.id }

        val maxTempPerSensor = keyed.reduce { r1, r2 ->
            if (r1.temperature > r2.temperature) r1 else r2
        }

        maxTempPerSensor.print()

        env.execute("Keyed Transformations Example")
    }
}