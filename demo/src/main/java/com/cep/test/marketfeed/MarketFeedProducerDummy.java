package com.cep.test.marketfeed;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MarketFeedProducerDummy implements IMarketFeedProducer, SourceFunction<MarketFeed> {
    private volatile boolean stopGenerator = false;
    private String [] symbolList = {
        "RELIANCE", "INFY", "WIPRO", "PNB", "IDEA", "NIFTYBEES", "KK", "LG", "SONY", "VIVO", "TVS", "TESLA"
    };

    public MarketFeedProducerDummy()
    {}

    @Override
    public MarketFeed next()
    {
        MarketFeed mf = new MarketFeed((double)((long)java.lang.Math.random() * 500 + 100), "RELIANCE", "NSECM");
        return mf;
    }

    @Override
    public boolean hasNext()
    {
        return true;
    }

    @Override
    public void run(SourceFunction.SourceContext<MarketFeed> ctx)
    {
        for (int i = 0; !stopGenerator; ++i)
        {
            synchronized (ctx.getCheckpointLock())
            {
                ctx.collect(next());
            }

            if (i == 1000)
            {
                ctx.markAsTemporarilyIdle();
            }
        }
    }

    @Override
    public void cancel()
    {
        stopGenerator = true;
    }
}
