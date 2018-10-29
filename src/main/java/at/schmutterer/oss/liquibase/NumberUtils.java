package at.schmutterer.oss.liquibase;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NumberUtils {

    public static Long toLong(Number number) {
        if (number == null) {
            return null;
        }
        return number.longValue();
    }

    public static Boolean toBoolean(Number number) {
        if (number == null) {
            return null;
        }
        return number.intValue() == 1;
    }

}

