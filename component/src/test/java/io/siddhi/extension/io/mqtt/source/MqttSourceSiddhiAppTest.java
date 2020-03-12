package io.siddhi.extension.io.mqtt.source;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.source.Source;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.testng.AssertJUnit;

public class MqttSourceSiddhiAppTest {

    @Test
    public void mqttSourceSiddhiAppTest() {
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan') " +
            "@source(type='mqtt', client.id='test_id', url='tcp://localhost:1883'," +
            "topic='mqtt_source_siddhi',clean.session='true', keep.alive='60', max.inflight='9'," +
            "automatic.reconnect='true', max.reconnect.delay='200000',@map(type='json'))" +
            "Define stream FooStream2 (symbol string, price float, volume long);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        Collection<List<Source>> sources = siddhiAppRuntime.getSources();

        sources.forEach( l -> l.forEach(s -> {
            MqttSource mqttSource = (MqttSource) s;
            AssertJUnit.assertEquals("tcp://localhost:1883", mqttSource.getBrokerURL());
            AssertJUnit.assertEquals("mqtt_source_siddhi", mqttSource.getTopicOption());
            AssertJUnit.assertEquals("test_id", mqttSource.getClientId());
            AssertJUnit.assertNull(mqttSource.getUserName());
            AssertJUnit.assertEquals("", mqttSource.getUserPassword());
            AssertJUnit.assertEquals("1", mqttSource.getQosOption());
            AssertJUnit.assertTrue(mqttSource.getCleanSession());
            AssertJUnit.assertEquals(60, mqttSource.getKeepAlive());
            AssertJUnit.assertEquals(30, mqttSource.getConnectionTimeout());
            AssertJUnit.assertEquals(9, mqttSource.getMaxInFlight());
            AssertJUnit.assertTrue(mqttSource.getAutomaticReconnect());
            AssertJUnit.assertEquals(200000, mqttSource.getMaxReconnectDelay());
        }));
    }
}
