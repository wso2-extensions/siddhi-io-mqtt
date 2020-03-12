package io.siddhi.extension.io.mqtt.sink;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.output.sink.Sink;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.testng.AssertJUnit;

public class MqttSinkSiddhiAppTest {

    @Test
    public void mqttSinkSiddhiAppTest() {
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan') " +
            "@sink(type='mqtt', client.id='test_id', url='tcp://localhost:1883'," +
            "topic='mqtt_source_siddhi',clean.session='true', keep.alive='60', max.inflight='9'," +
            "automatic.reconnect='true', max.reconnect.delay='200000',@map(type='json'))" +
            "Define stream FooStream2 (symbol string, price float, volume long);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        Collection<List<Sink>> sinks = siddhiAppRuntime.getSinks();

        sinks.forEach( l -> l.forEach(s -> {
            MqttSink mqttSink = (MqttSink) s;
            AssertJUnit.assertEquals("tcp://localhost:1883", mqttSink.getBrokerURL());
            AssertJUnit.assertEquals("mqtt_source_siddhi", mqttSink.getTopicOption());
            AssertJUnit.assertEquals("test_id", mqttSink.getClientId());
            AssertJUnit.assertNull(mqttSink.getUserName());
            AssertJUnit.assertEquals("", mqttSink.getUserPassword());
            AssertJUnit.assertEquals("1", mqttSink.getQosOption());
            AssertJUnit.assertTrue(mqttSink.getCleanSession());
            AssertJUnit.assertEquals(60, mqttSink.getKeepAlive());
            AssertJUnit.assertEquals(30, mqttSink.getConnectionTimeout());
            AssertJUnit.assertEquals(9, mqttSink.getMaxInFlight());
            AssertJUnit.assertTrue(mqttSink.getAutomaticReconnect());
            AssertJUnit.assertEquals(200000, mqttSink.getMaxReconnectDelay());
            AssertJUnit.assertEquals("false", mqttSink.getMessageRetain());
        }));
    }
}
