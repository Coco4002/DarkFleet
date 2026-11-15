package com.darkfleet.detection.support;

/** IMO ship identification numbers: seven digits, the last a check digit. */
public final class Imo {

    private Imo() {
    }

    /**
     * True for a well-formed IMO number: seven digits, not starting with 0, whose first six digits
     * weighted 7 down to 2 and summed end in the seventh digit. Crews mistyping the number is common,
     * and a mistyped number identifies nothing.
     */
    public static boolean isValid(String imo) {
        if (imo == null || !imo.matches("[1-9][0-9]{6}")) {
            return false;
        }
        int sum = 0;
        for (int i = 0; i < 6; i++) {
            sum += (imo.charAt(i) - '0') * (7 - i);
        }
        return sum % 10 == imo.charAt(6) - '0';
    }
}
