package net.vandenberge.metrics.kairosdb;

import net.vandenberge.metrics.kairosdb.KairosDbReporter;

import org.junit.Assert;
import org.junit.Test;

public class KairosDbReporterTest {

	/**
	 * Tags names and values can contain alphanumeric characters, slash, period, dash and underscore.
	 */
	@Test
	public void tags() {
		KairosDbReporter.Builder.validateTag("Valid");
		KairosDbReporter.Builder.validateTag("Valid.");
		KairosDbReporter.Builder.validateTag("Valid./");
		KairosDbReporter.Builder.validateTag("Valid./-");
		KairosDbReporter.Builder.validateTag("Valid./-__");
		KairosDbReporter.Builder.validateTag("_Va-//lid.");
	}
	
	@Test
	public void invalidTags() {
		assertInvalid(null);
		assertInvalid("");
		assertInvalid("invalid!");
		assertInvalid("{invalid}");
	}
	
	private static void assertInvalid(String tag) {
		try {
			KairosDbReporter.Builder.validateTag(tag);
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// Expected
		}
	}
}
