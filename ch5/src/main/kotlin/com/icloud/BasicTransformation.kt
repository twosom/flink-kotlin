package com.icloud

import com.icloud.extention.flatMap
import com.icloud.extention.typeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner

object BasicTransformation {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.config.autoWatermarkInterval = 1_000L

        val readings =
            env.addSource(SensorSource())
                .assignTimestampsAndWatermarks(SensorTimeAssigner())

        val filteredSensor =
            readings.filter { it.temperature >= 25 }

        val sensorIds =
            filteredSensor.map { it.id }

        val splitIds =
            sensorIds.flatMap(String::class.typeInformation()) { value, context ->
                value.split("_").forEach { context.collect(it) }
            }

        splitIds.print()

        env.execute("Basic Transformations Example")
    }
}