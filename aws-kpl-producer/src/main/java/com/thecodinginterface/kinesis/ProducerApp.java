package com.thecodinginterface.kinesis;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import io.debezium.config.Configuration;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.embedded.Connect;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;


public class ProducerApp implements Runnable{
    static final Logger logger = LogManager.getLogger(ProducerApp.class);
    static final ObjectMapper objMapper = new ObjectMapper();

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    
    public JsonConverter valueConverter;

    String streamName ="test_2";
    String region = "us-east-1";

    Configuration customconnector; 
    KinesisProducer producer;
    DebeziumEngine<RecordChangeEvent<SourceRecord>> engine; 

    public io.debezium.config.Configuration customerConnector() {

        Configuration config = io.debezium.config.Configuration.empty().withSystemProperties(Function.identity()).edit()
            .with("name", "customer-mysql-connector")
            .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
            .with("offset.storage.file.filename","offsets.dat")
            .with("offset.flush.interval.ms", "60000")
            .with("include.schema.changes", "false")
            .with("database.allowPublicKeyRetrieval", "true")
            .with("database.server.id", "85744")
            .with("database.server.name", "customer-mysql-db-server")
            .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
            .with("database.history.file.filename", "log.dat")
            .with("schemas.enable", false)
            .build();

        valueConverter = new JsonConverter();
        valueConverter.configure(config.asMap(), false);

        return config;
    }

    public DebeziumEngine<RecordChangeEvent<SourceRecord>> create_engine(
        io.debezium.config.Configuration customerConnector) {
            return
            DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
            .using(customerConnector.asProperties())
            .notifying(this::sendRecord)
            .build();
        }

    private void sendRecord(RecordChangeEvent<SourceRecord> sourceRecordRecordChangeEvent) {

            SourceRecord record = sourceRecordRecordChangeEvent.record();
            System.out.println(record.value());

            Schema schema = null;

            if ( null == record.keySchema() ) {
                System.out.println("The keySchema is missing. Something is wrong.");
                return;
            }

                   // For deletes, the value node is null
            if ( null != record.valueSchema() ) {
                schema = SchemaBuilder.struct()
                        .field("key", record.keySchema())
                        .field("value", record.valueSchema())
                        .build();
            }
            else {
                schema = SchemaBuilder.struct()
                        .field("key", record.keySchema())
                        .build();
            }

            Struct message = new Struct(schema);
            message.put("key", record.key());

            if ( null != record.value() )
                message.put("value", record.value());

            message.validate();

            byte[] payload;
            payload = valueConverter.fromConnectData("", schema, message);   // Transforms Record     
            System.out.println(payload);

            // // Starts KPL
            // List<Future<UserRecordResult>> putFutures = new LinkedList<Future<UserRecordResult>>();


            // // serialize and add to producer to be batched and sent to Kinesis
            // ByteBuffer data = null;
            // data = ByteBuffer.wrap(payload);
        
            // String partitionKey = String.valueOf(record.key() != null ? record.key().hashCode() : -1);

            // ListenableFuture<UserRecordResult> future = this.producer.addUserRecord(
            //     streamName,
            //     partitionKey,
            //     data
            // );
            // putFutures.add(future);

            // // register a callback handler on the async Future
            // Futures.addCallback(future, new FutureCallback<UserRecordResult>() {
            //     @Override
            //     public void onFailure(Throwable t) {
            //         logger.error("Failed to produce batch", t);
            //     }

            //     @Override
            //     public void onSuccess(UserRecordResult result) {
            //         logger.info(String.format("Produced User Record to shard %s at position %s",
            //                             result.getShardId(),
            //                             result.getSequenceNumber()));
            //     }
            // }, MoreExecutors.directExecutor());
        }
        
    @Override 
    public void run(){

        // var producerConfig = new KinesisProducerConfiguration().setRegion(region);

        // // instantiate KPL client
        // this.producer = new KinesisProducer(producerConfig);
        // Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        //     logger.info("Shutting down program");
        //     producer.flush();
        // }, "producer-shutdown"));

        customconnector = customerConnector();
        engine = create_engine(customconnector);
        this.executor.execute(engine);
        this.executor.shutdown();// the submitted task keeps running, only no more new ones can be added
    }
// export JAVA_TOOL_OPTIONS="-Xmx1024m"

 
    public static void main(String[] args) {
        new ProducerApp().run();

    }
}
