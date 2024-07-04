package com.icloud

import com.icloud.extention.max
import com.icloud.extention.valueState
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.LoggerFactory
import java.time.Instant

class OneSecondIntervalTrigger<InputT>
    : Trigger<InputT, TimeWindow>() {
    companion object {
        private val LOG = LoggerFactory.getLogger(OneSecondIntervalTrigger::class.java)
    }

    override fun onElement(
        element: InputT,
        timestamp: Long,
        window: TimeWindow,
        ctx: TriggerContext,
    ): TriggerResult {
        val firstSeenState: ValueState<Boolean> =
            ctx.getPartitionedState("first_seen".valueState(Boolean::class))

        val countState: ValueState<Long> =
            ctx.getPartitionedState("count".valueState(Long::class))

        val isFirstSeen = firstSeenState.value() ?: false

        if (!isFirstSeen) {
            val t = ctx.currentWatermark + 1_000 - (ctx.currentWatermark % 1_000)
            ctx.registerEventTimeTimer(t)
            ctx.registerEventTimeTimer(window.end)
            firstSeenState.update(true)
            LOG.info("[onElement] register event time timer at ${Instant.now()}")
        }

        var currentCount = countState.value() ?: 0
        currentCount += 1

        countState.update(currentCount)
            .also { LOG.info("[onElement] current count = {}", currentCount) }


        return TriggerResult.CONTINUE
    }

    override fun onEventTime(
        timestamp: Long,
        window: TimeWindow,
        ctx: TriggerContext,
    ): TriggerResult {
        if (timestamp == window.end) {
            return TriggerResult.FIRE_AND_PURGE
        }

        val t = ctx.currentWatermark + 1_000 - (ctx.currentWatermark % 1_000)
        if (t < window.end) {
            ctx.registerEventTimeTimer(t)
            LOG.info("[onEventTime] register event time timer at ${Instant.now()}")
        }

        return TriggerResult.FIRE
    }

    override fun onProcessingTime(
        time: Long,
        window: TimeWindow,
        ctx: TriggerContext,
    ): TriggerResult {
        return TriggerResult.CONTINUE
    }

    override fun clear(
        window: TimeWindow,
        ctx: TriggerContext,
    ) {
        ctx.getPartitionedState("first_seen".valueState(Boolean::class)).clear()
        ctx.getPartitionedState("count".valueState(Long::class)).clear()
        LOG.info("[clear] state clear at ${Instant.now()}")
    }

}

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    env.checkpointConfig.checkpointInterval = 10_000

    env.config.autoWatermarkInterval = 1000

    val readings = env
        .addSource(SensorSource())
        .assignTimestampsAndWatermarks(SensorTimeAssigner())


    readings.keyBy { it.id }
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .trigger(OneSecondIntervalTrigger())
        .reduce { r1, r2 ->
            r1.copy(
                timestamp = r1.timestamp.max(r2.timestamp),
                temperature = r1.temperature.max(r2.temperature)
            )
        }
        .print()


    env.execute()
}