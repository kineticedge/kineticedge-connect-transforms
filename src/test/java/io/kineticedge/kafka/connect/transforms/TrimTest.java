package io.kineticedge.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TrimTest {


    @Test
    public void testSchemalessAll() {

        try (Trim<SourceRecord> trim = new Trim.Value<>()) {

            trim.configure(Map.of("mode", "all"));

            Map<String, Object> value = new HashMap<>(
                    Map.ofEntries(
                            Map.entry("a", "a "),
                            Map.entry("b", " b"),
                            Map.entry("c", " c "),
                            Map.entry("d", "\td\n\r"),
                            Map.entry("nested", new HashMap<>(
                                            Map.ofEntries(
                                                    Map.entry("1", " 1 "),
                                                    Map.entry("2", "2"),
                                                    Map.entry("a", "   a   ")
                                            )
                                    )
                            ),
                            Map.entry("array", List.of(
                                            new HashMap<>(
                                                    Map.ofEntries(
                                                            Map.entry("list1", " 1 ")
                                                    )
                                            ),
                                            new HashMap<>(
                                                    Map.ofEntries(
                                                            Map.entry("list2", " 1 ")
                                                    )
                                            )
                                    )
                            )
                    )
            );

            final SourceRecord sourceRecord = new SourceRecord(null, null, "topic", 0, null, null, null, value);

            SourceRecord result = trim.apply(sourceRecord);

            assertEquals(
                    Map.ofEntries(
                            Map.entry("a", "a"),
                            Map.entry("b", "b"),
                            Map.entry("c", "c"),
                            Map.entry("d", "d"),
                            Map.entry("nested", Map.ofEntries(
                                            Map.entry("1", "1"),
                                            Map.entry("2", "2"),
                                            Map.entry("a", "a")
                                    )
                            ),
                            Map.entry("array", List.of(
                                            new HashMap<>(
                                                    Map.ofEntries(
                                                            Map.entry("list1", "1")
                                                    )
                                            ),
                                            new HashMap<>(
                                                    Map.ofEntries(
                                                            Map.entry("list2", "1")
                                                    )
                                            )
                                    )
                            )
                    ),
                    sourceRecord.value()
            );

            System.out.println(value);
        }
    }

    @Test
    public void testSchemalessTop() {

        try (Trim<SourceRecord> trim = new Trim.Value<>()) {

            trim.configure(Map.of("mode", "top"));

            Map<String, Object> value = new HashMap<>(Map.ofEntries(
                    Map.entry("a", "a "),
                    Map.entry("b", " b"),
                    Map.entry("c", " c "),
                    Map.entry("d", "\td\n\r"),
                    Map.entry("nested", new HashMap<>(
                                    Map.ofEntries(
                                            Map.entry("1", " 1 "),
                                            Map.entry("2", "2"),
                                            Map.entry("a", "   a   ")
                                    )
                            )
                    )
            ));

            final SourceRecord sourceRecord = new SourceRecord(null, null, "topic", 0, null, null, null, value);

            SourceRecord result = trim.apply(sourceRecord);

            assertEquals(
                    Map.ofEntries(
                            Map.entry("a", "a"),
                            Map.entry("b", "b"),
                            Map.entry("c", "c"),
                            Map.entry("d", "d"),
                            Map.entry("nested", Map.ofEntries(
                                            Map.entry("1", " 1 "),
                                            Map.entry("2", "2"),
                                            Map.entry("a", "   a   ")
                                    )
                            )
                    ),
                    sourceRecord.value()
            );

            System.out.println(value);
        }
    }


    @Test
    public void testSchemaAll() {

        Schema schema = SchemaBuilder
                .struct()
                .field("a", Schema.OPTIONAL_STRING_SCHEMA)
                .field("b", Schema.STRING_SCHEMA)
                .field("nested", SchemaBuilder.struct()
                        .field("a", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("b", Schema.OPTIONAL_STRING_SCHEMA)
                )
                .build();

        try (Trim<SourceRecord> trim = new Trim.Value<>()) {

            trim.configure(Map.of("mode", "all"));

            Struct struct = new Struct(schema);
            Struct nestedStruct = new Struct(schema.field("nested").schema());

            nestedStruct.put("a", "\t\ta\t\t");
            nestedStruct.put("b", "\t\tb  \n");



            struct.put("a", "a ");
            struct.put("b", " b");
            struct.put("nested", nestedStruct);


            final SourceRecord sourceRecord = new SourceRecord(null, null, "topic", 0, null, null, schema, struct);

            SourceRecord result = trim.apply(sourceRecord);

            System.out.println(struct);
//            assertEquals(
//                    Map.ofEntries(
//                            Map.entry("a", "a"),
//                            Map.entry("b", "b"),
//                            Map.entry("c", "c"),
//                            Map.entry("d", "d"),
//                            Map.entry("nested", Map.ofEntries(
//                                            Map.entry("1", "1"),
//                                            Map.entry("2", "2"),
//                                            Map.entry("a", "a")
//                                    )
//                            ),
//                            Map.entry("array", List.of(
//                                            new HashMap<>(
//                                                    Map.ofEntries(
//                                                            Map.entry("list1", "1")
//                                                    )
//                                            ),
//                                            new HashMap<>(
//                                                    Map.ofEntries(
//                                                            Map.entry("list2", "1")
//                                                    )
//                                            )
//                                    )
//                            )
//                    ),
//                    sourceRecord.value()
//            );
        }
    }

}