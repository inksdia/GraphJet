package com.spr.Utils;

import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * Created by saurav on 27/03/17.
 */
public final class Preconditions {

    private Preconditions() {
    }

    public static <E extends Enum<E>> EnumSet<E> emptySetIfNull(EnumSet<E> set, Class<E> klass) {
        if (set == null) {
            return EnumSet.noneOf(klass);
        }
        return set;
    }

    public static <T> Set<T> emptySetIfNull(Set<T> set) {
        if (set == null) {
            return Collections.emptySet();
        }
        return set;
    }

    public static <K, V> Map<K, V> isNotEmpty(Map<K, V> map) {
        return isNotEmpty(map, "Map cannot be empty or null");
    }

    public static <K, V> Map<K, V> isNotEmpty(Map<K, V> map, String errorMessage) {
        if (map == null || map.size() <= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return map;
    }

    public static <T> T notNullOrDefault(T value, T defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    public static <E, T extends Collection<E>> T isNotEmpty(T collection) {
        return isNotEmpty(collection, "Cannot be empty or null");
    }

    public static <E, T extends Collection<E>> T isNotEmpty(T collection, String errorMessage) {
        if (collection == null || collection.size() <= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return collection;
    }

    public static String blankIfNull(String input) {
        if (input == null) {
            return "";
        }
        return input;
    }

    public static <T extends Collection> T notEmpty(T coll) {
        return notEmpty(coll, "");
    }

    public static <T extends Collection> T notEmpty(T coll, String msg) {
        if (coll == null || coll.size() <= 0) {
            if (StringUtils.isBlank(msg)) {
                msg = "Collection cannot be empty";
            }
            throw new IllegalArgumentException(msg);
        }
        return coll;
    }

    public static <K, V> Map<K, V> notEmpty(Map<K, V> map, String msg) {
        if (map == null || map.size() <= 0) {
            if (StringUtils.isBlank(msg)) {
                msg = "Map cannot be empty";
            }
            throw new IllegalArgumentException(msg);
        }
        return map;
    }

    public static <T extends Collection> T notEmptyAndNotNullElements(T coll) {
        return notEmptyAndNotNullElements(coll, "");
    }

    public static <T extends Collection> T notEmptyAndNotNullElements(T coll, String msg) {
        msg = msg == null ? "" : msg;
        if (coll == null || coll.size() <= 0) {
            if (StringUtils.isBlank(msg)) {
                msg = "Collection cannot be empty";
            }
            throw new IllegalArgumentException(msg);
        }
        int index = 0;
        for (Object o : coll) {
            notNull(o, "Null element in position: " + index + " " + msg);
            index++;
        }
        return coll;
    }

    public static <T extends Collection<String>> T notEmptyAndNotBlankElements(T coll, String msg) {
        msg = msg == null ? "" : msg;
        if (coll == null || coll.size() <= 0) {
            if (StringUtils.isBlank(msg)) {
                msg = "Collection cannot be empty";
            }
            throw new IllegalArgumentException(msg);
        }
        int index = 0;
        for (String o : coll) {
            notBlank(o, "Blank string in position: " + index + " " + msg);
            index++;
        }
        return coll;
    }

    public static void isTrue(boolean expression, String msg) {
        if (!expression) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void isFalse(boolean expression, String msg) {
        if (expression) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static String notBlank(String value) {
        return notBlank(value, null);
    }

    public static String notBlank(String value, Object msg) {
        if (StringUtils.isNotBlank(value)) {
            return value;
        }
        throw new IllegalArgumentException(msg == null ? "Cannot be blank" : String.valueOf(msg));
    }

    public static void isBlank(String value, String msg) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        throw new IllegalArgumentException(msg == null ? "Must be blank" : String.valueOf(msg));
    }

    public static int positiveInt(Integer value) {
        return positiveInt(value, "value");
    }

    public static int positiveInt(Integer value, String name) {
        if (value != null && value > 0) {
            return value;
        }
        throw new IllegalArgumentException(name + " should be positive - " + value);
    }

    public static int nonPositiveInt(int value) {
        if (value <= 0) {
            return value;
        }
        throw new IllegalArgumentException("Value should be non-positive (lte 0) - " + value);
    }

    public static int negativeInt(int value) {
        if (value < 0) {
            return value;
        }
        throw new IllegalArgumentException("Value should be negative - " + value);
    }

    public static int nonNegativeInt(int value) {
        return nonNegativeInt(value, "unknown");

    }

    public static int nonNegativeInt(int value, String name) {
        if (value >= 0) {
            return value;
        }
        throw new IllegalArgumentException(String.format("Value for %s should be non-negative (gte 0) - %d", name, value));
    }

    public static long negativeLong(long value) {
        if (value < 0) {
            return value;
        }
        throw new IllegalArgumentException("Value should be negative - " + value);
    }

    public static long nonNegativeLong(long value) {
        if (value >= 0) {
            return value;
        }
        throw new IllegalArgumentException("Value should be non-negative (gte 0) - " + value);
    }

    public static long nonNegativeLong(long value, String name) {
        if (value >= 0) {
            return value;
        }
        throw new IllegalArgumentException(String.format("Value for %s should be non-negative (gte 0) - %d", name, value));
    }

    public static <T> void isNull(T instance, String msg) {
        if (instance == null) {
            return;
        }
        if (StringUtils.isBlank(msg)) {
            msg = "Should be null";
        }
        throw new IllegalArgumentException(msg);
    }

    public static <T> T notNull(T instance, String msg) {
        if (instance != null) {
            return instance;
        }
        if (StringUtils.isBlank(msg)) {
            msg = "Cannot be null";
        }
        throw new IllegalArgumentException(msg);
    }

    public static <T> T notNull(T instance) {
        if (instance != null) {
            return instance;
        }
        throw new IllegalArgumentException("Cannot be null");
    }

    public static <T> T[] notEmptyArray(T[] values) {
        return notEmptyArray(values, "Field");
    }

    public static <T> T[] notEmptyArray(T[] values, Object msg) {
        if (values == null || values.length <= 0) {
            throw new IllegalArgumentException(msg + " - cannot be empty array");
        }
        return values;
    }

    public static long positiveLong(Long value) {
        return positiveLong(value, "");
    }

    public static long positiveLong(Long value, String name) {
        if (value != null && value > 0) {
            return value;
        }
        if (StringUtils.isBlank(name)) {
            name = "Value";
        }
        throw new IllegalArgumentException(name + " should be positive - " + value);
    }

    public static double positiveDouble(Double value) {
        return positiveDouble(value, "");
    }

    public static double positiveDouble(Double value, String name) {
        if (value != null && value > 0) {
            return value;
        }
        if (StringUtils.isBlank(name)) {
            name = "Value";
        }
        throw new IllegalArgumentException(name + " should be positive - " + value);
    }

    public static <K, V> Map<K, V> emptyMapIfNull(Map<K, V> map) {
        if (map == null) {
            return Collections.emptyMap();
        }
        return map;
    }

    public static <T> List<T> emptyListIfNull(List<T> list) {
        if (list == null || list.size() == 0) {
            return Collections.emptyList();
        }
        return list;
    }

    public static <T> void containedInList(T object, List<T> list) {
        String message = object + " must be present in " + list;
        containedInList(object, list, message);
    }

    public static <T> void containedInList(T object, List<T> list, String message) {
        if (list == null || !list.contains(object)) {
            throw new IllegalArgumentException(message);
        }
    }

    public static <T> void containedInSet(T object, Set<T> set) {
        String message = object + " must be present in " + set;
        containedInSet(object, set, message);
    }

    public static <T> void containedInSet(T object, Set<T> set, String message) {
        if (set == null || !set.contains(object)) {
            throw new IllegalArgumentException(message);
        }
    }


    public static long longValueOf(String input, String errorMsg) {
        try {
            return Long.valueOf(input);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse long from '" + input + "' - " + errorMsg);
        }
    }

    public static int intValueOf(String input, String errorMsg) {
        try {
            return Integer.valueOf(input);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse int from '" + input + "' - " + errorMsg);
        }
    }

    public static boolean booleanValueOf(String input, String errorMsg) {
        try {
            return Boolean.valueOf(input);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse boolean from '" + input + "' - " + errorMsg);
        }
    }

    public static double doubleValueOf(String input, String errorMsg) {
        try {
            return Double.valueOf(input);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse double from '" + input + "' - " + errorMsg);
        }
    }

    public static float floatValueOf(String input, String errorMsg) {
        try {
            return Float.valueOf(input);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse float from '" + input + "' - " + errorMsg);
        }
    }

    public static Long longValueOrNull(String strLongValue) {
        try {
            return Long.parseLong(strLongValue);
        } catch (NumberFormatException e) {
            return null;
        }
    }


    public static float floatValueOrDefault(String val, float defaultVal) {
        try {
            return Float.parseFloat(val);
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }
}
