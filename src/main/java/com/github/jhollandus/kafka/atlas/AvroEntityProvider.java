package com.github.jhollandus.kafka.atlas;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvroEntityProvider {

    public static final String ATLAS_TYPE_AVRO_SCHEMA = "avro_schema";
    public static final String ATLAS_TYPE_AVRO_ARRAY = "avro_array";
    public static final String ATLAS_TYPE_AVRO_PRIMITIVE = "avro_primitive";
    public static final String ATLAS_TYPE_AVRO_FIELD = "avro_field";

    private static final Joiner NAME_JOINER = Joiner.on('.');

    private final Map<String, Referenceable> typeMap = new HashMap<>();

    public List<Referenceable> allEntities() {
        return new LinkedList<>(typeMap.values());
    }

    public Referenceable avroType(Schema avroSchema, String defNameSpace) {

        switch(avroSchema.getType()) {
            case RECORD: case ENUM:
                return typeMap.computeIfAbsent(avroSchema.getFullName(), key -> {
                    Referenceable schema = new Referenceable(ATLAS_TYPE_AVRO_SCHEMA);

                    String namespace = MoreObjects.firstNonNull(avroSchema.getNamespace(), defNameSpace);
                    schema.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, NAME_JOINER.join(namespace, avroSchema.getName()));
                    schema.set("name", avroSchema.getName());
                    schema.set("namespace", namespace);
                    schema.set("type", avroSchema.getType().getName());
                    schema.set("doc", avroSchema.getDoc());
                    schema.set("aliases", avroSchema.getAliases());

                    if(avroSchema.getType() == Schema.Type.RECORD) {
                        List<Referenceable> fields = resolveFields(typeMap, avroSchema, defNameSpace);
                        schema.set("fields", fields);
                    } else {
                        schema.set("symbols", avroSchema.getEnumSymbols());
                    }

                    return schema;
                });
            case ARRAY: case MAP:
                Referenceable schema = new Referenceable(ATLAS_TYPE_AVRO_ARRAY);
                schema.set("type", avroSchema.getType().getName());
                schema.set("items", resolveSchemas(typeMap, avroSchema.getTypes(), defNameSpace));
                return schema;
            case UNION:
                throw new IllegalArgumentException("Union is not a schema");
            default:
                Referenceable primitive = new Referenceable(ATLAS_TYPE_AVRO_PRIMITIVE);
                primitive.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, avroSchema.getFullName());
                primitive.set("name", avroSchema.getName());
                typeMap.put(avroSchema.getFullName(), primitive);
                return primitive;
        }
    }

    private List<Referenceable> resolveSchemas(Map<String, Referenceable> typeMap, List<Schema> avroSchemas, String defNameSpace) {
        return avroSchemas.stream().map(s -> avroType(s, defNameSpace)).collect(Collectors.toList());
    }

    private List<Referenceable> resolveFields(Map<String, Referenceable> typeMap, Schema avroSchema, String defNameSpace) {
        List<Referenceable> fields = new LinkedList<>();
        for(Schema.Field aField: avroSchema.getFields()) {
            Referenceable field = new Referenceable(ATLAS_TYPE_AVRO_FIELD);
            field.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, NAME_JOINER.join(avroSchema.getFullName(), aField.name()));
            field.set("name", aField.name());
            field.set("doc", aField.doc());

            if(aField.defaultVal() != null) {
                field.set("default", aField.defaultVal());
            }

            Schema aSchema = aField.schema();
            if(aSchema.getType() == Schema.Type.UNION) {
                field.set("type", resolveSchemas(typeMap, aSchema.getTypes(), defNameSpace));
            } else {
                field.set("type", avroType(aSchema, defNameSpace));
            }

            fields.add(field);
        }

        return fields;
    }
}
