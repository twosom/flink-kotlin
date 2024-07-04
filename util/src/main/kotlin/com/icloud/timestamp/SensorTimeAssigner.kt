package com.icloud.timestamp

import com.icloud.model.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

class SensorTimeAssigner
    : BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(5L)) {
    override fun extractTimestamp(element: SensorReading) = element.timestamp
}