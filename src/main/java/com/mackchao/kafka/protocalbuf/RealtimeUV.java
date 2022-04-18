//package com.mackchao.kafka.protocalbuf;
//
//import akka.remote.serialization.ProtobufSerializer;
//import org.apache.flink.addons.hbase.HBaseOptions;
//import org.apache.flink.addons.hbase.HBaseTableSchema;
//import org.apache.flink.addons.hbase.HBaseUpsertTableSink;
//import org.apache.flink.addons.hbase.HBaseWriteOptions;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.environment.CheckpointConfig;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
//import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
//
//import java.util.*;
//
///**
// * @author Mackchao.Sun
// * @description:
// * @date:2022/4/13
// **/
//public class RealtimeUV {
//
// public static void main(String[] args) throws Exception {
//  //step1 从properties配置文件中解析出需要的Kakfa、Hbase配置信息、checkpoint参数信息
//  Map<String, String> config = PropertiesUtil.loadConfFromFile(args[0]);
//  String topic = config.get("source.kafka.topic");
//  String groupId = config.get("source.group.id");
//  String sourceBootStrapServers = config.get("source.bootstrap.servers");
//  String hbaseTable = config.get("hbase.table.name");
//  String hbaseZkQuorum = config.get("hbase.zk.quorum");
//  String hbaseZkParent = config.get("hbase.zk.parent");
//  int checkPointPeriod = Integer.parseInt(config.get("checkpoint.period"));
//  int checkPointTimeout = Integer.parseInt(config.get("checkpoint.timeout"));
//
//  StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//  //step2 设置Checkpoint相关参数，用于Failover容错
//  sEnv.getConfig().registerTypeWithKryoSerializer(MobilePage.class,
//          ProtobufSerializer.class);
//  sEnv.getCheckpointConfig().setFailOnCheckpointingErrors(false);
//  sEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//  sEnv.enableCheckpointing(checkPointPeriod, CheckpointingMode.EXACTLY_ONCE);
//  sEnv.getCheckpointConfig().setCheckpointTimeout(checkPointTimeout);
//  sEnv.getCheckpointConfig().enableExternalizedCheckpoints(
//          CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//  //step3 使用Blink planner、创建TableEnvironment,并且设置状态过期时间，避免Job OOM
//  EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
//          .useBlinkPlanner()
//          .inStreamingMode()
//          .build();
//  StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, environmentSettings);
//  tEnv.getConfig().setIdleStateRetentionTime(Time.days(1), Time.days(2));
//
//  Properties sourceProperties = new Properties();
//  sourceProperties.setProperty("bootstrap.servers", sourceBootStrapServers);
//  sourceProperties.setProperty("auto.commit.interval.ms", "3000");
//  sourceProperties.setProperty("group.id", groupId);
//
//  //step4 初始化KafkaTableSource的Schema信息，笔者这里使用register TableSource的方式将源表注册到Flink中，而没有用register DataStream方式，也是因为想熟悉一下如何注册KafkaTableSource到Flink中
//  TableSchema schema = TableSchemaUtil.getAppPageViewTableSchema();
//  Optional<String> proctimeAttribute = Optional.empty();
//  List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors = Collections.emptyList();
//  Map<String, String> fieldMapping = new HashMap<>();
//  List<String> columnNames = new ArrayList<>();
//  RowTypeInfo rowTypeInfo = new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames());
//  columnNames.addAll(Arrays.asList(schema.getFieldNames()));
//  columnNames.forEach(name -> fieldMapping.put(name, name));
//  PageViewDeserializationSchema deserializationSchema = new PageViewDeserializationSchema(
//          rowTypeInfo);
//  Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
//  Kafka011TableSource kafkaTableSource = new Kafka011TableSource(
//          schema,
//          proctimeAttribute,
//          rowtimeAttributeDescriptors,
//          Optional.of(fieldMapping),
//          topic,
//          sourceProperties,
//          deserializationSchema,
//          StartupMode.EARLIEST,
//          specificOffsets);
//  tEnv.registerTableSource("pageview", kafkaTableSource);
//
//  //step5 初始化Hbase TableSchema、写入参数，并将其注册到Flink中
//  HBaseTableSchema hBaseTableSchema = new HBaseTableSchema();
//  hBaseTableSchema.setRowKey("log_date", String.class);
//  hBaseTableSchema.addColumn("f", "UV", Long.class);
//  HBaseOptions hBaseOptions = HBaseOptions.builder()
//          .setTableName(hbaseTable)
//          .setZkQuorum(hbaseZkQuorum)
//          .setZkNodeParent(hbaseZkParent)
//          .build();
//  HBaseWriteOptions hBaseWriteOptions = HBaseWriteOptions.builder()
//          .setBufferFlushMaxRows(1000)
//          .setBufferFlushIntervalMillis(1000)
//          .build();
//  HBaseUpsertTableSink hBaseSink = new HBaseUpsertTableSink(hBaseTableSchema, hBaseOptions, hBaseWriteOptions);
//  tEnv.registerTableSink("uv_index", hBaseSink);
//
//  //step6 实时计算当天UV指标sql, 这里使用最简单的group by agg，没有使用minibatch或窗口，在大数据量优化时最好使用后两种方式
//  String uvQuery = "insert into uv_index "
//          + "select log_date,\n"
//          + "ROW(count(distinct mid) as UV)\n"
//          + "from pageview\n"
//          + "group by log_date";
//  tEnv.sqlUpdate(uvQuery);
//  //step7 执行Job
//  sEnv.execute("UV Job");
// }
//}