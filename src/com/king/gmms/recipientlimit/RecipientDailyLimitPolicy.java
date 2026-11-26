package com.king.gmms.recipientlimit;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Immutable country override policy for the recipient 24-hour send limit.
 */
public final class RecipientDailyLimitPolicy {
    private static final Pattern COUNTRY_CODE_PATTERN = Pattern.compile("[A-Z]{2}");
    private static final Set<String> ISO_COUNTRY_CODES = createIsoCountryCodes();

    private final int defaultLimit;
    private final Map<String, Integer> countryLimits;

    private RecipientDailyLimitPolicy(int defaultLimit, Map<String, Integer> countryLimits) {
        this.defaultLimit = defaultLimit;
        this.countryLimits = Collections.unmodifiableMap(
                new HashMap<String, Integer>(countryLimits));
    }

    public static RecipientDailyLimitPolicy parse(int defaultLimit, String configuration) {
        Map<String, Integer> overrides = new HashMap<String, Integer>();
        if (configuration == null || configuration.trim().length() == 0) {
            return new RecipientDailyLimitPolicy(defaultLimit, overrides);
        }

        String[] groups = configuration.split(";");
        for (int i = 0; i < groups.length; i++) {
            parseGroup(groups[i], overrides);
        }
        return new RecipientDailyLimitPolicy(defaultLimit, overrides);
    }

    private static void parseGroup(String group, Map<String, Integer> overrides) {
        String value = group == null ? "" : group.trim();
        if (value.startsWith("(") && value.endsWith(")")) {
            value = value.substring(1, value.length() - 1).trim();
        }
        if (value.length() == 0) {
            throw new IllegalArgumentException("Empty country limit group");
        }

        String country = null;
        Integer maxCount = null;
        String[] attributes = value.split(",");
        for (int i = 0; i < attributes.length; i++) {
            String attribute = attributes[i].trim();
            int separator = attribute.indexOf('=');
            if (separator <= 0 || separator == attribute.length() - 1) {
                throw new IllegalArgumentException("Invalid country limit attribute: " + attribute);
            }
            String name = attribute.substring(0, separator).trim();
            String attributeValue = attribute.substring(separator + 1).trim();
            if ("Country".equalsIgnoreCase(name)) {
                if (country != null) {
                    throw new IllegalArgumentException("Duplicate Country attribute: " + group);
                }
                country = normalizeCountry(attributeValue);
            } else if ("MaxCount".equalsIgnoreCase(name)) {
                if (maxCount != null) {
                    throw new IllegalArgumentException("Duplicate MaxCount attribute: " + group);
                }
                maxCount = parseMaxCount(attributeValue);
            } else {
                throw new IllegalArgumentException("Unknown country limit attribute: " + name);
            }
        }

        if (country == null || maxCount == null) {
            throw new IllegalArgumentException("Country and MaxCount are required: " + group);
        }
        if (overrides.containsKey(country)) {
            throw new IllegalArgumentException("Duplicate country limit: " + country);
        }
        overrides.put(country, maxCount);
    }

    private static String normalizeCountry(String country) {
        String normalized = country == null ? "" : country.trim().toUpperCase(Locale.ENGLISH);
        if (!COUNTRY_CODE_PATTERN.matcher(normalized).matches()
                || !ISO_COUNTRY_CODES.contains(normalized)) {
            throw new IllegalArgumentException("Invalid ISO country code: " + country);
        }
        return normalized;
    }

    private static Set<String> createIsoCountryCodes() {
        Set<String> countries = new HashSet<String>();
        String[] isoCountries = Locale.getISOCountries();
        for (int i = 0; i < isoCountries.length; i++) {
            countries.add(isoCountries[i]);
        }
        return Collections.unmodifiableSet(countries);
    }

    private static Integer parseMaxCount(String maxCount) {
        try {
            int value = Integer.parseInt(maxCount);
            if (value < 0) {
                throw new IllegalArgumentException("MaxCount must be greater than or equal to zero: " + maxCount);
            }
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid MaxCount: " + maxCount, e);
        }
    }

    public int getLimit(String country) {
        if (country != null) {
            String normalized = country.trim().toUpperCase(Locale.ENGLISH);
            Integer countryLimit = countryLimits.get(normalized);
            if (countryLimit != null) {
                return countryLimit.intValue();
            }
        }
        return defaultLimit;
    }

    public int getCountryLimitCount() {
        return countryLimits.size();
    }
}
