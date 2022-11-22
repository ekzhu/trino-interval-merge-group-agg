/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.intervalmergegroupagg;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;

@AggregationFunction(value = "merge_group", decomposable = false, isOrderSensitive = true)
public final class IntervalMergeGroupAggregation
{
    private IntervalMergeGroupAggregation() {}

    @TypeParameter("T")
    @InputFunction
    public static void input(@AggregationState LongAndLong state, @SqlType("T") long start, @SqlType("T") long end)
    {
        // Initialize state.
        if (state.getFirst() == 0L && state.getSecond() == 0L) {
            state.setFirst(end);
            state.setSecond(1);
            return;
        }

        // First is the end value of the previous merge group.
        // Assume the intervals are coming in ascending order of start.
        if (start <= state.getFirst()) {
            state.setFirst(Math.max(state.getFirst(), end));
        }
        else {
            state.setFirst(end);
            // Second is the current number of merge group.
            state.setSecond(state.getSecond() + 1);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState LongAndLong state, @AggregationState LongAndLong otherState)
    {
        throw new UnsupportedOperationException("merge_group must run on a single machine");
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState LongAndLong state, BlockBuilder out)
    {
        long count = state.getSecond();
        if (count == 0) {
            out.appendNull();
        }
        else {
            // Output the current number of merge group found.
            BigintType.BIGINT.writeLong(out, count);
        }
    }
}
