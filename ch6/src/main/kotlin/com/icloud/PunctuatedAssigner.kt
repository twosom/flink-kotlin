package com.icloud

import com.icloud.model.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

object PunctuatedAssigner
    : AssignerWithPunctuatedWatermarks<SensorReading> {
    private const val BOUND: Long = 60 * 1_000

    private fun readResolve(): Any = PunctuatedAssigner

    override fun checkAndGetNextWatermark(
        lastElement: SensorReading,
        extractedTimestamp: Long,
    ): Watermark? = when (lastElement.id) {
        "sensor_1" -> Watermark(extractedTimestamp - BOUND)
        else -> null
    }

    override fun extractTimestamp(
        element: SensorReading,
        recordTimestamp: Long,
    ) = element.timestamp
}