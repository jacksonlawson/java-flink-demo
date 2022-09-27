package com.cep.test;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.eventtime.WatermarksWithIdleness;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.shaded.netty4.io.netty.util.internal.logging.InternalLogger;
import org.apache.flink.shaded.netty4.io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.source.TimestampsAndWatermarks;
import org.apache.flink.streaming.api.operators.source.TimestampsAndWatermarksContext;
import org.apache.flink.streaming.api.transformations.TimestampsAndWatermarksTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.cep.test.marketfeed.MarketFeedProducerDummy;
import com.cep.test.marketfeed.MarketFeed;
import com.cep.test.mtmtriggers.MTMTrigger;
import com.cep.test.mtmtriggers.MTMProducerDummy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * Hello world!
 *
 */
public class App 
{
	public static java.util.HashMap<String, Integer> countermap = new java.util.HashMap<String, Integer>();
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);

		DataStream<MTMTrigger> triggers = env.addSource(new MTMProducerDummy());
		DataStream<MarketFeed> feeds = env.addSource(new MarketFeedProducerDummy());

		triggers.print();
		feeds.print();

		DataStream<MTMTrigger> triggerstream = triggers.keyBy(new KeySelector<MTMTrigger, Integer>() {
			@Override
			public Integer getKey(MTMTrigger pmtm)
			{
				return pmtm.symbol.hashCode() / 5;
			}
		});

		DataStream<MarketFeed> feedsstream = feeds.keyBy(new KeySelector<MarketFeed, Integer>() {
			@Override
			public Integer getKey(MarketFeed pfeed)
			{
				return pfeed.symbol.hashCode() / 5;
			}
		});

		triggerstream.connect(feedsstream).process(new CoProcessFunction<MTMTrigger,MarketFeed,String>() {

			HashMap<String, HashMap<String, MTMTrigger>> symbolWiseTriggerToMatch = new HashMap<>();
			HashMap<String, Double> symbolWiseLTP = new HashMap<>();

			void applyMatch(Double pltp, HashMap<String, MTMTrigger> parr, Collector<String> pout)
			{
				// int matchCount = 0;
				if (parr != null)
				{
					parr.forEach((key, mtm) -> {
						if (mtm.side == "B")
						{
							if (mtm.price >= pltp && pltp != mtm.ltp)
							{
								String action = "BUY Trigger=>" + mtm.toString();
								pout.collect(action);
								// matchCount++;
								mtm.ltp = pltp;
							}
						}
						else
						{
							if (mtm.price <= pltp && pltp != mtm.ltp)
							{
								String action = "SELL Trigger=>" + mtm.toString();
								pout.collect(action);
								// matchCount++;
								mtm.ltp = pltp;
							}
						}
					});
				}

				// java.lang.System.out.println("Total match found = %d", matchCount);
			}

			@Override
			public void processElement1(MTMTrigger arg0, CoProcessFunction<MTMTrigger,MarketFeed,String>.Context arg1, Collector<String> arg2)
			{
				try {
					HashMap<String, MTMTrigger> arr = symbolWiseTriggerToMatch.get(arg0.symbol);
					if (arr == null)
					{
						arr = new HashMap<>();
						symbolWiseTriggerToMatch.put(arg0.symbol, arr);
					}

					String key = arg0.account + arg0.side;
					arr.put(key, arg0);

					Double ltp = symbolWiseLTP.get(arg0.symbol);

					if (ltp != null)
					{
						applyMatch(ltp, arr, arg2);
					}
				}
				catch (Exception ex)
				{
					System.err.println(ex.getMessage());
				}
			}

			@Override
			public void processElement2(MarketFeed arg0, CoProcessFunction<MTMTrigger,MarketFeed,String>.Context arg1, Collector<String> arg2)
			{
				try 
				{
					symbolWiseLTP.put(arg0.symbol, arg0.ltp);

					HashMap<String, MTMTrigger> arr = symbolWiseTriggerToMatch.get(arg0.symbol);
					if (arr != null)
					{
						applyMatch(arg0.ltp, arr, arg2);
					}
				}
				catch (Exception ex)
				{
					System.err.println(ex.getMessage());
				}
			}
  
			@Override
			public  void onTimer(long timestamp, CoProcessFunction<MTMTrigger,MarketFeed,String>.OnTimerContext ctx, Collector<String> out) 
			{
			}
		})
		.addSink(new SinkFunction<String>(){
			@Override
			public void invoke(String result)
			{
				InternalLogger logger = Slf4JLoggerFactory.getInstance("final-sink");
				logger.info("Match result=> %s", result);
			}
		});
		try 
		{
			env.execute("dummy-test");	
		}
		catch (Exception e)
		{
			System.err.println(e.getMessage());
		}
	 }

	 public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
		   for (String word: sentence.split(" ")) {
			  out.collect(new Tuple2<String, Integer>(word, 1));
		   }
		}
	}
}
