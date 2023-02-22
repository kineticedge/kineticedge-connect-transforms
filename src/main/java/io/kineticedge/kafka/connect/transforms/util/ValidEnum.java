package io.kineticedge.kafka.connect.transforms.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ValidEnum implements ConfigDef.Validator {

    private final Class<? extends Enum<?>> enumClass;
    private final Set<String> validEnums;

    private ValidEnum(final Class<? extends Enum<?>> enumClass, final Set<String> excludes) {

        if (enumClass == null) {
            throw new IllegalArgumentException("enumClass cannot be null");
        }

        this.enumClass = enumClass;
        this.validEnums = Arrays
                .stream(enumClass.getEnumConstants())
                .map(Enum::name)
                .filter(e -> !excludes.contains(e))
                .collect(Collectors.toSet());
    }

    public static ValidEnum of(Class<? extends Enum<?>> enumClass, String... excludes) {
        final Set<String> ex = (excludes != null) ? Arrays.stream(excludes).collect(Collectors.toSet()) : Collections.emptySet();
        return new ValidEnum(enumClass, ex);
    }

    public void ensureValid(final String config, final Object value) {
        if (value instanceof String) {
            if (!this.validEnums.contains(value)) {
                throw new ConfigException(config, String.format("'%s' is not a valid value for %s. Valid values are %s.", value, this.enumClass.getSimpleName(), String.join(", ", validEnums)));
            }
        } else if (value instanceof List) {
            ((List<?>) value).forEach(item -> ensureValid(config, item));
        } else {
                throw new ConfigException(config, value, "Must be a String or List");
        }
    }

    public String toString() {
        return "Matches: ``" + String.join(", ", this.validEnums) + "``";
    }

}