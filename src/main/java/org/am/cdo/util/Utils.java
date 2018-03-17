package org.am.cdo.util;

public class Utils {

	public static String formatTime(final long ms) {
	    long millis = ms % 1000;
	    long x = ms / 1000;
	    long seconds = x % 60;
	    x /= 60;
	    long minutes = x % 60;
	    x /= 60;
	    long hours = x % 24;
	    return String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds, millis);
	}
}
