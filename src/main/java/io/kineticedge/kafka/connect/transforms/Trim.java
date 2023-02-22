/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kineticedge.kafka.connect.transforms;

import io.kineticedge.kafka.connect.transforms.util.CollectionUtil;
import io.kineticedge.kafka.connect.transforms.util.ValidEnum;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 *
 *
 */
public abstract class Trim<R extends ConnectRecord<R>> implements Transformation<R> {

    public enum ModeType {
        top,
        list,
        all;
    }

    private static final Logger log = LoggerFactory.getLogger(Trim.class);

    public static final String MODE = "mode";
    public static final String FIELDS = "fields";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(MODE,
                    ConfigDef.Type.STRING,
                    ModeType.top.name(),
                    ValidEnum.of(ModeType.class),
                    ConfigDef.Importance.HIGH,
                    "the behavior to follow for trimming, the default, 'top', is to trim all top-level fields that are strings.")
            .define(FIELDS,
                    ConfigDef.Type.LIST,
                    null,
                    null,
                    ConfigDef.Importance.HIGH,
                    "list of fields to trim")
            ;

    private static final String PURPOSE = "trim strings";

    private ModeType mode = ModeType.top;
    private List<String> fields;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        this.fields = config.getList(FIELDS);
        this.mode = Enum.valueOf(ModeType.class, config.getString(MODE));

        if (this.mode == ModeType.list && CollectionUtil.isEmpty(this.fields)) {
            throw new ConfigException("for 'list' mode at least 1 field must be specified.");
        }
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        }

        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    private R applySchemaless(R record) {

        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        switch (mode) {
            case list:
                for (String field : fields) {
                    trimField(value, field);
                }
                break;
            case top:
                for (String field : value.keySet()) {
                    trimField(value, field);
                }
                break;
            case all:
                trimAll(value);
                break;
        }

        return record;
    }


    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        switch (mode) {
            case list:
                for (String field : fields) {
                    trimField(value, field);
                }
                break;
            case top:
                for (Field field : value.schema().fields()) {
                    trimField(value, field.name());
                }
                break;
            case all:
                trimAll(value);
                break;
        }

        return record;
    }

    @SuppressWarnings("unchecked")
    private static void trimAll(final Map<String, Object> map) {
        for (String field : map.keySet()) {
            if (map.get(field) instanceof String) {
                String value = (String) map.get(field);
                map.put(field, value.trim());
            } else if (map.get(field) instanceof Map) {
                Map<String, Object> value = (Map<String, Object>) map.get(field);
                trimAll(value);
            }
        }
    }

    private static void trimField(final Map<String, Object> map, final String fieldName) {
        if (map.get(fieldName) instanceof String) {
            map.put(fieldName, ((String) map.get(fieldName)).trim());
        }
    }

    private static void trimAll(final Struct struct) {

        if (struct == null) {
            return;
        }

        for (Field field : struct.schema().fields()) {
            if (field.schema().type() == Schema.Type.STRING) {
                String value = struct.getString(field.name());
                if (value != null) {
                    struct.put(field.name(), value.trim());
                }
            } else if (field.schema().type() == Schema.Type.STRUCT) {
                trimAll(struct.getStruct(field.name()));
            } else if (field.schema().type() == Schema.Type.ARRAY) {
                if (field.schema().valueSchema().type() == Schema.Type.STRING) {
                    log.warn("an array of strings is not trimmed.");
                } else if (field.schema().valueSchema().type() == Schema.Type.STRUCT) {
                    List<Object> list = struct.getArray(field.name());
                    if (list != null) {
                        for (Object object : list) {
                            trimAll((Struct) object);
                        }
                    }
                }
            } else if (field.schema().type() == Schema.Type.MAP) {
                log.warn("structure has Map elements which currently are not searched in this transformer.");
            }

        }
    }

    private static void trimField(final Struct struct, final String fieldName) {
        final Field field = struct.schema().field(fieldName);
        if (field != null && field.schema().type() == Schema.Type.STRING) {
            if (struct.get(fieldName) instanceof String) {
                struct.put(fieldName, ((String) struct.get(fieldName)).trim());
            }
        }
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static final class Key<R extends ConnectRecord<R>> extends Trim<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends Trim<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
