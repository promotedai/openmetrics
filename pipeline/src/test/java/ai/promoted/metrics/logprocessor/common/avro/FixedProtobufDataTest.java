package ai.promoted.metrics.logprocessor.common.avro;

import ai.promoted.proto.common.FlatProperties;
import ai.promoted.proto.common.Properties;
import ai.promoted.proto.delivery.FlatQualityScoreConfig;
import ai.promoted.proto.delivery.QualityScoreConfig;
import ai.promoted.proto.delivery.QualityScoreTerm;
import ai.promoted.proto.delivery.QualityScoreTerm1;
import ai.promoted.proto.delivery.QualityScoreTerms;
import ai.promoted.proto.delivery.QualityScoreTerms1;
import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtobufData;
import org.junit.jupiter.api.Test;
import com.google.protobuf.Descriptors.Descriptor;

import static org.junit.jupiter.api.Assertions.*;

public class FixedProtobufDataTest {

    @Test
    public void testGetSchema_Properties() {
        Class c = Properties.class;
        Schema schema = null;
        try {
            Object descriptor = c.getMethod("getDescriptor").invoke((Object)null);
            schema = FixedProtobufData.get().getSchema((Descriptor)descriptor);
        } catch (Exception e) {
            fail(e);
        }
        assertTrue(schema.toString().contains("FlatProperties"));
        assertTrue(schema.toString().contains("Struct1"));
    }

    @Test
    public void testGetSchema_QualityScoreConfig() {
        Class c = QualityScoreConfig.class;
        Schema schema = null;
        try {
            Object descriptor = c.getMethod("getDescriptor").invoke((Object)null);
            schema = FixedProtobufData.get().getSchema((Descriptor)descriptor);
        } catch (Exception e) {
            fail(e);
        }
        assertTrue(schema.toString().contains("FlatQualityScoreConfig"));
        assertTrue(schema.toString().contains("QualityScoreTerm1"));
    }

    @Test
    public void testResolveUnionNormal_Properties() {
        Schema schemaProperties = ProtobufData.get().getSchema(Properties.class);
        Schema schemaStruct = schemaProperties.getField("struct").schema();
        Properties p = Properties.newBuilder().build();
        int i = FixedProtobufData.get().resolveUnion(schemaStruct, p.getStruct());
        assertEquals(1, i);
    }

    @Test
    public void testResolveUnionModified_FlatProperties() {
        Schema schemaProperties = ProtobufData.get().getSchema(FlatProperties.class);
        Schema schemaStruct = schemaProperties.getField("struct").schema();
        Properties p = Properties.newBuilder().build();
        int i = FixedProtobufData.get().resolveUnion(schemaStruct, p.getStruct());
        assertEquals(1, i);
    }

    @Test
    public void testResolveUnionNormal_QualityScoreTerm() {
        Schema schemaProperties = ProtobufData.get().getSchema(QualityScoreTerm.class);
        Schema schemaStruct = schemaProperties.getField("product").schema();
        QualityScoreTerm t = QualityScoreTerm.newBuilder().build();
        int i = FixedProtobufData.get().resolveUnion(schemaStruct, t.getProduct());
        assertEquals(1, i);
    }

    @Test
    public void testResolveUnionModified_QualityScoreTerm() {
        Schema schemaProperties = ProtobufData.get().getSchema(QualityScoreTerm1.class);
        Schema schemaStruct = schemaProperties.getField("product").schema();
        QualityScoreTerm1 t = QualityScoreTerm1.newBuilder().build();
        int i = FixedProtobufData.get().resolveUnion(schemaStruct, t.getProduct());
        assertEquals(1, i);
    }
}
