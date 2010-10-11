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

	private final int f1 = 2;
	private final int f2 = 4;
	private final int f3 = 6;

	private final int p1 = 20;
	private final int p2 = 40;

	private final int movingBoundary = 20;
	
	private static FuzzyInferenceEngine instance = new FuzzyInferenceEngine();
	
	public static FuzzyInferenceEngine getInstance() {
		return instance;
	}
	
	private FuzzyInferenceEngine() {
		
	}
	
	public long estimateNextPeriodLength(int nrViewChanges, long previousPeriodLength) {
		
		int p = (int) (previousPeriodLength/1000);
		if (nrViewChanges == 0 ) 
			return (p + movingBoundary)*1000;
		else if (veryHighViewChanges(nrViewChanges) && veryFrequentPeriod(p))
			return 5*1000;
		else if (veryHighViewChanges(nrViewChanges) && frequentPeriod(p))
			return 10*1000;
		else if (veryHighViewChanges(nrViewChanges) && lessFrequentPeriod(p))
			return 15*1000;
		else if (highViewChanges(nrViewChanges) && veryFrequentPeriod(p))
			return 20*1000;
		else if (highViewChanges(nrViewChanges) && frequentPeriod(p))
			return 25*1000;
		else if (highViewChanges(nrViewChanges) && lessFrequentPeriod(p))
			return 30*1000;
		else if (lowViewChanges(nrViewChanges) && veryFrequentPeriod(p))
			return 35*1000;
		else if (lowViewChanges(nrViewChanges) && frequentPeriod(p))
			return 40*1000;
		else if (lowViewChanges(nrViewChanges) && lessFrequentPeriod(p))
			return 45*1000;
		else if (veryLowViewChanges(nrViewChanges) && veryFrequentPeriod(p))
			return 50*1000;
		else if (veryLowViewChanges(nrViewChanges) && frequentPeriod(p))
			return 55*1000;
		else if (veryLowViewChanges(nrViewChanges) && lessFrequentPeriod(p))
			return 60*1000;
		else
			return 65*1000;
		
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
	
	public int getMovingBoundary() {
		return movingBoundary;
	}
}
