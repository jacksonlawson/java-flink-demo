package com.cep.test.marketfeed;

public class MarketFeed {
    public double ltp = 0;
    public String symbol = "";
    public String exchange = "";
    public long timestampInMicro = 0;

    public MarketFeed(double pltp, String psymbol, String pexchange)
    {
        ltp = pltp;
        symbol = psymbol;
        exchange = pexchange;
    }

    @Override
    public String toString() {
        return symbol + "," + Double.toString(ltp);
    }
}
