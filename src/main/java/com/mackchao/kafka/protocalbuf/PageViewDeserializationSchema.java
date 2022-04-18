//package com.mackchao.kafka.protocalbuf;
//
//import org.apache.flink.api.common.serialization.DeserializationSchema;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.types.Row;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.text.SimpleDateFormat;
//import java.util.Locale;
//
///**
// * @author Mackchao.Sun
// * @description:
// * @date:2022/4/13
// **/
//public class PageViewDeserializationSchema implements DeserializationSchema<Row> {
//
//    public static final Logger LOG = LoggerFactory.getLogger(PageViewDeserializationSchema.class);
//    protected SimpleDateFormat dayFormatter;
//
//    private final RowTypeInfo rowTypeInfo;
//
//    public PageViewDeserializationSchema(RowTypeInfo rowTypeInfo){
//        dayFormatter = new SimpleDateFormat("yyyyMMdd", Locale.UK);
//        this.rowTypeInfo = rowTypeInfo;
//    }
//    @Override
//    public Row deserialize(byte[] message) throws IOException {
//        Row row = new Row(rowTypeInfo.getArity());
//        MobilePage mobilePage = null;//
//        try {
//            mobilePage = MobilePage.parseFrom(message);
//            String mid = mobilePage.getMid();
//            row.setField(0, mid);
//            Long timeLocal = mobilePage.getTimeLocal();
//            String logDate = dayFormatter.format(timeLocal);
//            row.setField(1, logDate);
//            row.setField(2, timeLocal);
//        }catch (Exception e){
//            String mobilePageError = (mobilePage != null) ? mobilePage.toString() : "";
//            LOG.error("error parse bytes payload is {}, pageview error is {}", message.toString(), mobilePageError, e);
//        }
//        return null;
//    }