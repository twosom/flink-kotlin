package com.icloud

import com.icloud.extention.logger
import com.icloud.model.SensorReading
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import java.io.Serializable

object ListCheckpointedExample {

    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.checkpointConfig.checkpointInterval = 10 * 1000

        env.config.autoWatermarkInterval = 1000

        val sensorData = env
            .addSource(SensorSource())
            .assignTimestampsAndWatermarks(SensorTimeAssigner())

        val highTempCounts = sensorData
            .keyBy { it.id }
            .flatMap(HighTempCounter(threshold = 120.0))

        highTempCounts.print("HIGH_TEMP_COUNTS")

        env.execute("ListCheckPointed Example")
    }

    private data class TempCount(
        private val subtaskIdx: Int,
        private val count: Long,
    ) : Serializable


    private class HighTempCounter(
        private val threshold: Double,
    ) : RichFlatMapFunction<SensorReading, TempCount>(), @Suppress("DEPRECATION") ListCheckpointed<Long> {

        companion object {
            private val LOG = HighTempCounter::class.logger()
        }

        private var subtaskIdx: Int = 0

        override fun open(
            parameters: Configuration,
        ) {
            this.subtaskIdx = this.runtimeContext.indexOfThisSubtask
        }

        private var highTempCount = 0L


        override fun flatMap(
            value: SensorReading,
            out: Collector<TempCount>,
        ) {
            if (value.temperature > threshold) {
                this.highTempCount += 1
                out.collect(TempCount(subtaskIdx, highTempCount))
            }
        }

        override fun restoreState(
            state: MutableList<Long>,
        ) {
            this.highTempCount = state.sum()
                .also { LOG.info("restoreState triggered...") }
        }

        override fun snapshotState(
            checkpointId: Long,
            timestamp: Long,
        ): MutableList<Long> = mutableListOf(this.highTempCount)
            .also { LOG.info("snapshotState triggered...") }

    }


}