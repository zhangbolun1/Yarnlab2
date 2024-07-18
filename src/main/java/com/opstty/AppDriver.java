package com.opstty;

import com.opstty.job.TreeJob;
import com.opstty.job.SpeciesJob;
import com.opstty.job.TreeKindJob;
import com.opstty.job.MaxHeightJob;
import com.opstty.job.SortHeightsJob;
import com.opstty.job.OldestTreeJob;
import com.opstty.job.TreeCountJob;
import com.opstty.job.MaxTreeJob;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("treejob", TreeJob.class,
                    "A map/reduce program that processes tree data.");
            programDriver.addClass("speciesjob", SpeciesJob.class,
                    "A map/reduce program that lists different species of trees.");
            programDriver.addClass("treekindjob", TreeKindJob.class,
                    "A map/reduce program that counts the number of trees of each kind.");
            programDriver.addClass("maxheightjob", MaxHeightJob.class,
                    "A map/reduce program that calculates the height of the tallest tree of each kind.");
            programDriver.addClass("sortheightsjob", SortHeightsJob.class,
                    "A map/reduce program that sorts the trees height from smallest to largest.");
            programDriver.addClass("oldesttreejob", OldestTreeJob.class,
                    "A map/reduce program that finds the district with the oldest tree.");
            programDriver.addClass("treecountjob", TreeCountJob.class,
                    "A map/reduce program that counts the number of trees in each district.");
            programDriver.addClass("maxtreejob", MaxTreeJob.class,
                    "A map/reduce program that finds the district with the most trees.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}