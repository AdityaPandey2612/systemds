/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.test.component.ooc.memory;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.planning.OOCMaterializedInputRequest;
import org.apache.sysds.runtime.ooc.primitives.GeneralMMultOOCPrimitive;
import org.apache.sysds.runtime.ooc.stats.OOCEventLog;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.utils.Statistics;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class GeneralMMultPrimitiveParityTest {
	private static final int M = 1000;
	private static final int K = 20000;
	private static final int N = 50000;
	private static final int BLEN = 250;
	private static final long WAIT_TIMEOUT_SEC = 600;
	private static final long MIB = 1024L * 1024L;
	private static final Map<BEvictionPolicy, String> POLICY_RESULTS = new ConcurrentHashMap<>();

	private final BEvictionPolicy _bEvictionPolicy;
	private boolean _oldOOCStatistics;
	private long _startNanos;

	private enum BEvictionPolicy {
		FORWARD,
		REVERSE,
		NEUTRAL;

		private long score(long index) {
			return switch(this) {
				case FORWARD -> index;
				case REVERSE -> -index;
				case NEUTRAL -> 0;
			};
		}
	}

	@Parameterized.Parameters(name = "B eviction policy: {0}")
	public static Collection<Object[]> policies() {
		return Arrays.asList(new Object[][] {
			{BEvictionPolicy.FORWARD},
			{BEvictionPolicy.REVERSE},
			{BEvictionPolicy.NEUTRAL}
		});
	}

	public GeneralMMultPrimitiveParityTest(BEvictionPolicy bEvictionPolicy) {
		_bEvictionPolicy = bEvictionPolicy;
	}

	@Before
	public void setUp() {
		_oldOOCStatistics = DMLScript.OOC_STATISTICS;
		DMLScript.OOC_STATISTICS = true;
		//Statistics.resetOOCEvictionStats();

		//DMLScript.OOC_STATISTICS = true;
		DMLScript.OOC_LOG_EVENTS = true;
		DMLScript.OOC_LOG_PATH   = "/Users/adityapandey/Documents/OOC-Exp-Results";
		OOCEventLog.setup(10_000_000);
		Statistics.resetOOCEvictionStats();
		OOCCacheManager.getGlobalCache().updateLimits(16 * MIB, 8 * MIB);
		_startNanos = System.nanoTime();
	}

	//baseline could be one tile column major and other row major
	// what could be next future tile that is needed
	//counter to count how many times it is visited
	// minimize evict writes, and loadfromdisk
	// how much would it help to keep the tiles in the disk
	// it needs to independent of the memory cost function - invariant
	// how can you experiment with a known vs  unknown cache sizes and how much would we loose by not knowing the cache size, not much
	// lets say operator assumed 1gig of memory but turns out we have 100, does it break the metric, or messes up the policy

	// start by fixed cache limit

	@After
	public void tearDown() {
		try {
			double elapsedSeconds = (System.nanoTime() - _startNanos) / 1e9;
			String result = String.format("Policy: %s%nElapsed time: %.3f sec%n%s",
				_bEvictionPolicy, elapsedSeconds, Statistics.displayOOCEvictionStats());
			POLICY_RESULTS.put(_bEvictionPolicy, result);
			System.out.println(result);
		}
		finally {
			try {
				OOCCacheManager.reset();
			}
			finally {
				DMLScript.OOC_STATISTICS = _oldOOCStatistics;
			}
		}
	}

	@AfterClass
	public static void displayPolicyComparison() {
		System.out.println("\n========== EVICTION POLICY COMPARISON ==========");
		for(BEvictionPolicy policy : BEvictionPolicy.values()) {
			String result = POLICY_RESULTS.get(policy);
			System.out.println(result != null ? result : "No result for policy " + policy);
			System.out.println("------------------------------------------------");
		}
	}

	@Test
	public void testMaterializedBGeneralMultiply() throws Exception {
		OOCStream<IndexedMatrixValue> a = matrixStream(M, K);
		OOCInstructionUtils.dataGen(a,
			ix -> constantTile(ix, M, K, 2.0), new StreamContext(0, "general_mm_a").addOutStream(a));

		OOCStream<IndexedMatrixValue> b = matrixStream(K, N);
		OOCInstructionUtils.dataGen(b,
			ix -> constantTile(ix, K, N, 3.0), new StreamContext(0, "general_mm_b").addOutStream(b));

		OOCStream<IndexedMatrixValue> out = matrixStream(M, N);
		AggregateOperator aggregate = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator mm = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), aggregate);

		// Previous primitive creation, retained for comparison:
		// OOCInstructionUtils.matrixMultiply(a, b, out, mm, new BinaryOperator(Plus.getPlusFnObject()),
		// 	new StreamContext(0, "general_mm").addOutStream(out));

		final int bColBlocks = Math.toIntExact(OOCUtils.getNumColBlocks(b.getDataCharacteristics()));
		GeneralMMultOOCPrimitive primitive = new GeneralMMultOOCPrimitive(a, b, out, mm,
			new BinaryOperator(Plus.getPlusFnObject()),
			new StreamContext(0, "general_mm").addOutStream(out)) {
			@Override
			protected long bEvictionScore(MatrixIndexes indexes) {
				long index = (indexes.getRowIndex() - 1) * bColBlocks + indexes.getColumnIndex() - 1;
				return _bEvictionPolicy.score(index);
			}
		};
		out.assignPrimitive(primitive);

		Assert.assertTrue(out.getPrimitive() instanceof GeneralMMultOOCPrimitive);
		OOCMaterializedInputRequest request = out.getPrimitive().requiresMaterializedInput();
		Assert.assertEquals(1, request.inputIndex());
		int bRowBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(b.getDataCharacteristics()));
		MatrixIndexes probe = new MatrixIndexes(Math.min(2, bRowBlocks), Math.min(3, bColBlocks));
		int expectedLinearIndex = Math.toIntExact(
			(probe.getRowIndex() - 1) * bColBlocks + probe.getColumnIndex() - 1);
		int linearIndex = request.preferredLayout().linearize(probe);
		Assert.assertEquals(expectedLinearIndex, linearIndex);
		Assert.assertEquals(probe, request.preferredLayout().delinearize(linearIndex));
		Assert.assertTrue("Packed pins must be budgeted by physical rather than logical size.",
			OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(1) > 1);

		Map<MatrixIndexes, Double> sums = new ConcurrentHashMap<>();
		CompletableFuture<Void> done = new CompletableFuture<>();
		out.setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					done.complete(null);
					return;
				}
				IndexedMatrixValue value = cb.get();
				sums.put(new MatrixIndexes(value.getIndexes()), ((MatrixBlock)value.getValue()).sum());
			}
			catch(Throwable t) {
				done.completeExceptionally(t);
			}
			finally {
				cb.close();
			}
		});

		out.start();
		done.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);

		int outputRowBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(out.getDataCharacteristics()));
		int outputColBlocks = Math.toIntExact(OOCUtils.getNumColBlocks(out.getDataCharacteristics()));
		Assert.assertEquals(Math.multiplyExact(outputRowBlocks, outputColBlocks), sums.size());
		double expectedCellValue = 2.0 * 3.0 * K;
		for(int row = 1; row <= outputRowBlocks; row++) {
			for(int col = 1; col <= outputColBlocks; col++) {
				MatrixIndexes outputIndex = new MatrixIndexes(row, col);
				Double sum = sums.get(outputIndex);
				Assert.assertNotNull("Missing output tile (" + row + "," + col + ")", sum);
				double expectedTileSum = expectedCellValue *
					OOCUtils.getNumRowsOfTile(outputIndex, M, BLEN) *
					OOCUtils.getNumColsOfTile(outputIndex, N, BLEN);
				Assert.assertEquals(expectedTileSum, sum, 1e-9);
			}
		}
	}

	private static MatrixBlock constantTile(MatrixIndexes indexes, long rows, long cols, double value) {
		return new MatrixBlock(OOCUtils.getNumRowsOfTile(indexes, rows, BLEN),
			OOCUtils.getNumColsOfTile(indexes, cols, BLEN), value);
	}

	private static OOCStream<IndexedMatrixValue> matrixStream(long rows, long cols) {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		MatrixCharacteristics dc = new MatrixCharacteristics(rows, cols, BLEN, -1);
		stream.setData(new MatrixObject(ValueType.FP64, null, new MetaDataFormat(dc, FileFormat.BINARY)));
		return stream;
	}
}
