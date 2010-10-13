/**
 * Copyright 2009 - 2010 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.cluster.ensemble.impl;

import terrastore.cluster.ensemble.EnsembleConfiguration;

/**
 * @author Amir Moulavi
 */
public class FuzzyInferenceEngine {

    private final int f1 = 2;
    private final int f2 = 4;
    private final int f3 = 6;
    private long p1 = 20;
    private long p2 = 40;
    private long boundaryIncrement = 20;
    private long interval;
    private static FuzzyInferenceEngine instance = new FuzzyInferenceEngine();

    public static FuzzyInferenceEngine getInstance() {
        return instance;
    }

    private FuzzyInferenceEngine() {
    }

    public long getBoundaryIncrement() {
        return boundaryIncrement;
    }

    public long estimateNextPeriodLength(int nrViewChanges, long previousPeriodLength, EnsembleConfiguration.DiscoveryConfiguration conf) {

        calculateParametersFrom(conf);

        int p = (int) (previousPeriodLength / 1000);
        if (nrViewChanges == 0) {
            return (p + boundaryIncrement) * 1000;
        } else if (veryHighViewChanges(nrViewChanges) && veryFrequentPeriod(p)) {
            return interval * 1000;
        } else if (veryHighViewChanges(nrViewChanges) && frequentPeriod(p)) {
            return 2 * interval * 1000;
        } else if (veryHighViewChanges(nrViewChanges) && lessFrequentPeriod(p)) {
            return 3 * interval * 1000;
        } else if (highViewChanges(nrViewChanges) && veryFrequentPeriod(p)) {
            return 4 * interval * 1000;
        } else if (highViewChanges(nrViewChanges) && frequentPeriod(p)) {
            return 5 * interval * 1000;
        } else if (highViewChanges(nrViewChanges) && lessFrequentPeriod(p)) {
            return 6 * interval * 1000;
        } else if (lowViewChanges(nrViewChanges) && veryFrequentPeriod(p)) {
            return 7 * interval * 1000;
        } else if (lowViewChanges(nrViewChanges) && frequentPeriod(p)) {
            return 8 * interval * 1000;
        } else if (lowViewChanges(nrViewChanges) && lessFrequentPeriod(p)) {
            return 9 * interval * 1000;
        } else if (veryLowViewChanges(nrViewChanges) && veryFrequentPeriod(p)) {
            return 10 * interval * 1000;
        } else if (veryLowViewChanges(nrViewChanges) && frequentPeriod(p)) {
            return 11 * interval * 1000;
        } else if (veryLowViewChanges(nrViewChanges) && lessFrequentPeriod(p)) {
            return 12 * interval * 1000;
        } else {
            return 13 * interval * 1000;
        }

    }

    private void calculateParametersFrom(EnsembleConfiguration.DiscoveryConfiguration conf) {
        p1 = (conf.getBaseline() / 1000) / 2;
        p2 = (conf.getBaseline() / 1000) + p1;
        boundaryIncrement = conf.getBoundaryIncrement() / 1000;
        interval = (p1 + p2) / 12;
    }

    private boolean veryFrequentPeriod(int p) {
        return p < p1 ? true : false;
    }

    private boolean frequentPeriod(int p) {
        return p1 <= p && p < p2 ? true : false;
    }

    private boolean lessFrequentPeriod(int p) {
        return p2 <= p ? true : false;
    }

    private boolean veryLowViewChanges(int v) {
        return v < f1 ? true : false;
    }

    private boolean lowViewChanges(int v) {
        return f1 <= v && v < f2 ? true : false;
    }

    private boolean highViewChanges(int v) {
        return f2 <= v && v < f3 ? true : false;
    }

    private boolean veryHighViewChanges(int v) {
        return f3 <= v ? true : false;
    }
}
