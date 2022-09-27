package com.cep.test.mtmtriggers;

import java.lang.Math;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MTMProducerDummy implements IMTMProducer, SourceFunction<MTMTrigger> {
    private boolean stopGenerator = false;

    private String [] symbolList = {
        "RELIANCE", "INFY", "WIPRO", "PNB", "IDEA", "NIFTYBEES", "KK", "LG", "SONY", "VIVO", "TVS", "TESLA"
    };
    private String [] accountList = {
        "ABC123", "ABC124", "ABC125", "ABC126", "ABC127", "ABC128", "ABC129", "ABC130", "ABC131", "ABC132", "ABC133", "ABC134", "ABC135",
        "ABC136", "ABC137", "ABC138", "ABC139", "ABC140", "ABC141", "ABC142", "ABC143", "ABC144", "ABC145"
    };
    private String [] sideList = {"B", "S"};

    public MTMProducerDummy()
    { }

    @Override
    public void run(SourceFunction.SourceContext<MTMTrigger> ctx)
    {
        int count = 0;
        outter: for (int i = 0; !stopGenerator; ++i)
        {
            for (String a : accountList)
            {
                if (stopGenerator)
                {
                    break outter;
                }

                for (String b: symbolList)
                {
                    if (stopGenerator)
                    {
                        break outter;
                    }

                    for (String s: sideList)
                    {
                        if (stopGenerator)
                        {
                            break outter;
                        }

                        count++;
                        MTMTrigger mtm = new MTMTrigger(a, b, s, (double)((long)Math.random()*500+100));
                        synchronized (ctx.getCheckpointLock())
                        {
                            ctx.collect(mtm);
                        }

                        if (i == 1000)
                        {
                            ctx.markAsTemporarilyIdle();
                        }
                    }
                }
            }
            stopGenerator = true;
            // System.out.println("Successfully gnerated %d MTM Trigger objects", count);
        }
    }

    @Override
    public void cancel()
    {
        stopGenerator = true;
    }
}
