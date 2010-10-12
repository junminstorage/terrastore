package terrastore.cluster.ensemble.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import terrastore.cluster.ensemble.SchedulerConfiguration;

/**
 * 
 * @author Amir Moulavi
 *
 */

public class FuzzyInferenceEngineTest {

	private FuzzyInferenceEngine fuzzy;
	private long previousPeriodLength;
	private int viewChanges;
	private long result;
    private SchedulerConfiguration schedulerConf;

	@Before
	public void set_up() {
		fuzzy = FuzzyInferenceEngine.getInstance();
		schedulerConf = SchedulerConfiguration.makeDefault();
	}
	
	@Test
	public void no_view_change() {
		given(view_changes(0), previous_period_length(50));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(50 + fuzzy.getMovingBoundary());
	}

	@Test
	public void very_high_view_changes_and_very_frequent_period() {
		given(view_changes(7), previous_period_length(10));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(5);
	}

	@Test
	public void very_high_view_changes_and_frequent_period() {
		given(view_changes(7), previous_period_length(21));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(10);
	}

	@Test
	public void very_high_view_changes_and_less_frequent_period() {
		given(view_changes(7), previous_period_length(45));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(15);
	}

	@Test
	public void high_view_changes_and_very_frequent_period() {
		given(view_changes(5), previous_period_length(10));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(20);
	}

	@Test
	public void high_view_changes_and_frequent_period() {
		given(view_changes(5), previous_period_length(25));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(25);
	}

	@Test
	public void high_view_changes_and_less_frequent_period() {
		given(view_changes(5), previous_period_length(80));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(30);
	}

	@Test
	public void low_view_changes_and_very_frequent_period() {
		given(view_changes(3), previous_period_length(10));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(35);
	}

	@Test
	public void low_view_changes_and_frequent_period() {
		given(view_changes(3), previous_period_length(30));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(40);
	}

	@Test
	public void low_view_changes_and_less_frequent_period() {
		given(view_changes(3), previous_period_length(90));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(45);
	}

	@Test
	public void very_low_view_changes_and_very_frequent_period() {
		given(view_changes(1), previous_period_length(10));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(50);
	}

	@Test
	public void very_low_view_changes_and_frequent_period() {
		given(view_changes(1), previous_period_length(35));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(55);
	}

	@Test
	public void very_low_view_changes_and_less_frequent_period() {
		given(view_changes(1), previous_period_length(65));
		
		when_the_fuzzy_inference_engine_estimates();
		
		then_the_estimated_next_preiod_length_is(60);
	}
	
	private void given(int viewChanges, long previousPeriodLength) {
		this.viewChanges = viewChanges;
		this.previousPeriodLength = previousPeriodLength;		
	}

	private void when_the_fuzzy_inference_engine_estimates() {
		result = fuzzy.estimateNextPeriodLength(viewChanges, previousPeriodLength, schedulerConf);		
	}
	
	private void then_the_estimated_next_preiod_length_is(int estimatedPeriodLength) {
		Assert.assertTrue("Wrong estimated value: expected [" + estimatedPeriodLength*1000 +"], but it was [" + result +"]", estimatedPeriodLength*1000 == result);		
	}

	private int view_changes(int nrViewChanges) {
		return nrViewChanges;
	}
	
	private long previous_period_length(int period) {
		return period*1000;
	}
}
