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
package terrastore.cluster.ensemble;

/**
 * 
 * @author Amir Moulavi
 *
 */
public class SchedulerConfiguration {

    private int baseline;
    private int movingBoundry;

    public static SchedulerConfiguration makeDefault() {
        SchedulerConfiguration schedulerConf = new SchedulerConfiguration();
        schedulerConf.setBaseline(30);
        schedulerConf.setMovingBoundry(20);
        return schedulerConf;
    }

    public void validate() {
        validateBaseLine();
        validateMovingBoundary();
    }

    private void validateMovingBoundary() {
        if (movingBoundry < 0) {
            throw new EnsembleConfigurationException("MovingBoundary SchedulerConfiguration is not valid: " + movingBoundry);
        }
    }

    private void validateBaseLine() {
        if (baseline < 0) {
            throw new EnsembleConfigurationException("Baseline in SchedulerConfiguration is not valid: " + baseline);
        }
    }

    public int getBaseline() {
        return baseline;
    }

    public void setBaseline(int baseline) {
        this.baseline = baseline;
    }

    public int getMovingBoundry() {
        return movingBoundry;
    }

    public void setMovingBoundry(int movingBoundry) {
        this.movingBoundry = movingBoundry;
    }

}
