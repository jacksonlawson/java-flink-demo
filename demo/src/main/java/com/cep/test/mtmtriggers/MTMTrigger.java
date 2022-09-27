package com.cep.test.mtmtriggers;

public class MTMTrigger {
    public String account;
    public String symbol;
    public String side;
    public Double price;
    public Double ltp;

    public MTMTrigger(String paccount, String psymbol, String pside, Double pprice)
    {
        account = paccount;
        symbol = psymbol;
        side = pside;
        price = pprice;
        ltp = 0.0;
    }

    @Override
    public String toString() {
        return account + "," +  symbol + ", " + side + ", " + Double.toString(price) + ", " + Double.toString(ltp);
    }
}
