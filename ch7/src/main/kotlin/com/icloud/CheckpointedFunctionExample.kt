package com.icloud

import com.icloud.extention.listState
import com.icloud.extention.logger
import com.icloud.extention.valueNonNull
import com.icloud.extention.valueState
import com.icloud.model.SensorReading
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.CheckpointListener
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object CheckpointedFunctionExample {
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
            .flatMap(HighTempCounter(threshold = 10.0))

        highTempCounts.print()


        env.execute("Checkpointed Function Example")
    }

    data class HighTempCount(
        private val sensorId: String,
        private val keyedHighTempCount: Long,
        private val operationHighTempCount: Long,
    )

    private class HighTempCounter(
        private val threshold: Double,
    ) : FlatMapFunction<SensorReading, HighTempCount>, CheckpointedFunction, CheckpointListener {

        companion object {
            private val LOG = HighTempCounter::class.logger()
        }

        private var operatorHighTempCount: Long = 0
        private lateinit var keyedCountState: ValueState<Long>
        private lateinit var operatorCountState: ListState<Long>

        override fun flatMap(
            value: SensorReading,
            out: Collector<HighTempCount>,
        ) {
            if (value.temperature > threshold) {
                this.operatorHighTempCount += 1

                val keyedHighTempCount = this.keyedCountState.valueNonNull() + 1

                this.keyedCountState.update(keyedHighTempCount)

                out.collect(HighTempCount(value.id, keyedHighTempCount, operatorHighTempCount))
            }
        }

        override fun initializeState(
            context: FunctionInitializationContext,
        ) {
            this.keyedCountState = context.keyedStateStore.getState("keyed_count".valueState(Long::class))
            this.operatorCountState = context.operatorStateStore.getListState("operator_count".listState(Long::class))
                .also {
                    this.operatorHighTempCount = it.get().sum()
                    LOG.info("[initializeState] state initialized...")
                }
        }

        override fun snapshotState(
            context: FunctionSnapshotContext,
        ) {
            this.operatorCountState.clear()
            this.operatorCountState.add(this.operatorHighTempCount)
                .also { LOG.info("[snapshotState] state snapshot complete...") }
        }

        override fun notifyCheckpointComplete(
            checkpointId: Long,
        ) {
            LOG.info("[notifyCheckpointComplete] checkpoint $checkpointId completed...")
        }

    }
}