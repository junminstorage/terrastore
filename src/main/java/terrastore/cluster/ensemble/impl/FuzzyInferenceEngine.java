/**
 * 
 * @author Amir Moulavi <amir.moulavi@gmail.com>
 * 
 * 	  Licensed under the Apache License, Version 2.0 (the "License");
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


public class FuzzyInferenceEngine {

	private final int veryLow = 2;
	private final int low = 4;
	private final int high = 5;

	private final int veryFrequent = 20;
	private final int frequent = 40;

	private static FuzzyInferenceEngine instance = new FuzzyInferenceEngine();
	
	public static FuzzyInferenceEngine getInstance() {
		return instance;
	}
	
	private FuzzyInferenceEngine() {
		
	}
	
	public int estimateNextPeriodLength(int nrViewChanges, int previousPeriodLength) {
		
		if (veryHighViewChanges(nrViewChanges) && veryFrequentPeriod(previousPeriodLength))
			return 5;
		else if (veryHighViewChanges(nrViewChanges) && frequentPeriod(previousPeriodLength))
			return 10;
		else if (veryHighViewChanges(nrViewChanges) && lessFrequentPeriod(previousPeriodLength))
			return 15;
		else if (highViewChanges(nrViewChanges) && veryFrequentPeriod(previousPeriodLength))
			return 20;
		else if (highViewChanges(nrViewChanges) && frequentPeriod(previousPeriodLength))
			return 25;
		else if (highViewChanges(nrViewChanges) && lessFrequentPeriod(previousPeriodLength))
			return 30;
		else if (lowViewChanges(nrViewChanges) && veryFrequentPeriod(previousPeriodLength))
			return 35;
		else if (lowViewChanges(nrViewChanges) && frequentPeriod(previousPeriodLength))
			return 40;
		else if (lowViewChanges(nrViewChanges) && lessFrequentPeriod(previousPeriodLength))
			return 45;
		else if (veryLowViewChanges(nrViewChanges) && veryFrequentPeriod(previousPeriodLength))
			return 50;
		else if (veryLowViewChanges(nrViewChanges) && frequentPeriod(previousPeriodLength))
			return 55;
		else if (veryLowViewChanges(nrViewChanges) && lessFrequentPeriod(previousPeriodLength))
			return 60;
		else
			return 65;
		
	}
	
	private boolean veryFrequentPeriod(int p) {
		return p < veryFrequent ? true : false;
	}

	private boolean frequentPeriod(int p) {
		return veryFrequent <= p && p < frequent ? true : false;
	}

	private boolean lessFrequentPeriod(int p) {
		return frequent <= p ? true : false;
	}

	private boolean veryLowViewChanges(int v) {
		return v < veryLow ? true : false;
	}

	private boolean lowViewChanges(int v) {
		return veryLow <= v && v < low ? true : false;
	}

	private boolean highViewChanges(int v) {
		return low <= v && v < high ? true : false;
	}

	private boolean veryHighViewChanges(int v) {
		return high <= v ? true : false;
	}
	
	
}
