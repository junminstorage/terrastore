/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.router.impl;

import org.junit.Test;

/**
 * @author Sergio Bossa
 */
public class HashFunctionsTest {

    public HashFunctionsTest() {
    }

    @Test
    public void simpleHashFunctionTest() throws InterruptedException {
        HashFunction fn = new SimpleHashFunction();
        doTest(fn);
    }

    @Test
    public void djbHashFunctionTest() throws InterruptedException {
        HashFunction fn = new DJBHashFunction();
        doTest(fn);
    }

    @Test
    public void murmurHashFunctionTest() throws InterruptedException {
        HashFunction fn = new MurmurHashFunction();
        doTest(fn);
    }

    public void doTest(HashFunction fn) {
        int partitions = 1024;
        int iterations = 1000000;
        int[] occurrences = new int[partitions];
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            // Using System.nanoTime() to simulate similar keys:
            int hash = fn.hash("" + Math.abs(System.nanoTime()), partitions);
            occurrences[hash]++;
        }

        System.err.println("Tested: " + fn);
        System.err.println("Elapsed time in nanos: " + (System.nanoTime() - start));
        System.err.println("Standard deviation of hash occurrences (lower is better): " + standardDeviation(occurrences));
    }

    private double standardDeviation(int... values) {
        double mean = mean(values);

        double stdDeviation = 0;
        for (int value : values) {
            stdDeviation += (Math.pow(value - mean, 2));
        }
        stdDeviation = Math.sqrt(stdDeviation / values.length);
        return stdDeviation;
    }

    private double mean(int... values) {
        double mean = 0;
        for (int value : values) {
            mean += value;
        }
        mean = mean / values.length;
        return mean;
    }
}
