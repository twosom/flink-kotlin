package com.icloud

import com.icloud.extention.max
import com.icloud.model.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

object PeriodicAssigner
    : AssignerWithPeriodicWatermarks<SensorReading> {

    private fun readResolve(): Any = PeriodicAssigner

    private const val BOUND: Long = 60 * 1_000
    private var MAX_TS: Long = Long.MIN_VALUE

    override fun getCurrentWatermark(): Watermark =
        Watermark(this.MAX_TS - this.BOUND)

    override fun extractTimestamp(
        element: SensorReading,
        recordTimestamp: Long,
    ) = element.timestamp
        .apply { MAX_TS = this.max(MAX_TS) }


}