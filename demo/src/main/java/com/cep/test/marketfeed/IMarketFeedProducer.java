package com.cep.test.marketfeed;

public interface IMarketFeedProducer {
    abstract boolean hasNext();
    abstract MarketFeed next();
}
