public class MolocoQ1 {
    /**
     * @param x First String
     * @param y Second String
     * @return true if one string can be converted to the other by removing exactly 1 character
     */
    private static boolean equalsWhenOneCharRemoved(String x, String y) {
        if (x == null || y == null) {
            return false;
        }
        // we need to iterate over the shorter length string among the two and we assume String x to be the shorter
        if (x.length() > y.length()) {
            return equalsWhenOneCharRemoved(y, x);
        }
        int xLength = x.length();
        int yLength = y.length();

        // if abs difference between the strings is more than 1, there is no way we can obtain either string by removing
        // one character from other string, hence return false
        if (Math.abs(xLength - yLength) > 1) {
            return false;
        }
        // if string lengths are same, we cannot remove 1 character from either string to equal the other string, hence
        // return false
        if (xLength == yLength) {
            return false;
        }
        // iterate over shorter length string and compare character at each index with the string of longer length.
        // if there is a character mismatch, the substring of longer string after the mismatch character much be equal
        // to the substring of shorter string after mismatch character. If it is, return true, else return false
        for (int i = 0; i < xLength; i++) {
            if (x.charAt(i) != y.charAt(i)) {
                return x.substring(i).equals(y.substring(i + 1));
            }
        }

        // if there is no different in xLength, String x can be converted to String y, if String x has exactly 1
        // character more than String y
        return (xLength + 1) == yLength;
    }

    public static void main(String[] args) {
        System.out.println(equalsWhenOneCharRemoved("x", "y")); // false
        System.out.println(equalsWhenOneCharRemoved("x", "XX")); // false
        System.out.println(equalsWhenOneCharRemoved("yy", "yx")); // false
        System.out.println(equalsWhenOneCharRemoved("abcd", "abxcd")); // true
        System.out.println(equalsWhenOneCharRemoved("xyz","xz")); // true
        System.out.println(equalsWhenOneCharRemoved("", "x")); // true
        System.out.println(equalsWhenOneCharRemoved(null, "abc")); // false
        System.out.println(equalsWhenOneCharRemoved(null, null)); // false
    }
}
