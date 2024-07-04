package com.icloud

import com.icloud.model.SensorReading
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import com.icloud.source.SensorSource

object BoundedOutOfOrdernessTimestampExtractorExample {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        val stream: DataStream<SensorReading> = env.addSource(SensorSource())

        stream.assignTimestampsAndWatermarks(
            object : BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(10L)) {
                override fun extractTimestamp(element: SensorReading): Long =
                    element.timestamp
            }
        )



        env.execute("Bounded Out Of Orderness Timestamp Extractor Example")
    }
}