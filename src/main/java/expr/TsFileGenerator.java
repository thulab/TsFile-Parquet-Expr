package expr;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.DoubleDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.IntDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;
import datagen.DataGenerator;
import datagen.GeneratorFactory;
import hadoop.HDFSOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static cons.Constants.*;

public class TsFileGenerator {

    private TsFileWriter writer;
    private MonitorThread monitorThread;
    private long timeConsumption;
    private DataGenerator dataGenerator;

    static Logger logger = LoggerFactory.getLogger(TsFileGenerator.class);
    /**
     * initialize the data input file stream and  register all the sensors.
     * @param localFile whether saving the data on local disk. if false, then saving data on HDFS.
     * @throws WriteProcessException
     * @throws IOException
     */
    private void initWriter(boolean localFile) throws WriteProcessException, IOException {
        if(localFile){
            File file= new File(filePath);
            if(file.exists()){
                file.delete();
            }
            writer = new TsFileWriter(file);
        }else{
            writer = new TsFileWriter(new HDFSOutputStream(filePath, true));
        }
        for (int i = 0; i < sensorNum; i++) {
            MeasurementDescriptor descriptor = new MeasurementDescriptor(SENSOR_PREFIX + i, dataType, encoding);
            writer.addMeasurement(descriptor);
        }
    }

    private void gen(boolean localFile) throws IOException, WriteProcessException {
        long startTime = System.currentTimeMillis();
        monitorThread = new MonitorThread();
        monitorThread.start();
        initWriter(localFile);
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();
        for(int i = 0; i < ptNum; i ++) {
            Object value = dataGenerator.next();
            for(int j = 0; j < deviceNum; j ++) {
                TSRecord record = new TSRecord(i + 1, DEVICE_PREFIX + j);
                for (int k = 0; k < sensorNum; k++) {
                    DataPoint point = null;
                    switch (dataType) {
                        case DOUBLE:
                            point = new DoubleDataPoint(SENSOR_PREFIX + k, (double) value);
                            break;
                        case FLOAT:
                            point = new FloatDataPoint(SENSOR_PREFIX + k, (float) value);
                            break;
                        case INT32:
                            point = new IntDataPoint(SENSOR_PREFIX + k, (int) value);
                            break;
                        case INT64:
                            point = new LongDataPoint(SENSOR_PREFIX + k, (long) value);
                    }
                    record.addTuple(point);
                }
                writer.write(record);
            }
            if ((i + 1) % (ptNum / 100) == 0) {
                // System.out.println(String.format("Progress: %d%%", (i + 1)*100 / ptNum));
            }
        }
        writer.close();
        timeConsumption = System.currentTimeMillis() - startTime;
        writer = null;
        monitorThread.interrupt();

    }

    private void genNonalign(boolean localFile) throws IOException, WriteProcessException {
        long startTime = System.currentTimeMillis();
        monitorThread = new MonitorThread();
        monitorThread.start();
        initWriter(localFile);
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();
        for(int i = 0; i < ptNum; i ++) {
            Object value = dataGenerator.next();
            for(int j = 0; j < deviceNum; j ++) {
                for (int k = 0; k < sensorNum; k++) {
                    TSRecord record = new TSRecord((long) ((i + 1) * sensorNum + k), DEVICE_PREFIX + j);
                    DataPoint point = null;
                    switch (dataType) {
                        case DOUBLE:
                            point = new DoubleDataPoint(SENSOR_PREFIX + k, (double) value);
                            break;
                        case FLOAT:
                            point = new FloatDataPoint(SENSOR_PREFIX + k, (float) value);
                            break;
                        case INT32:
                            point = new IntDataPoint(SENSOR_PREFIX + k, (int) value);
                            break;
                        case INT64:
                            point = new LongDataPoint(SENSOR_PREFIX + k, (long) value);
                    }
                    record.addTuple(point);
                    writer.write(record);
                }
            }
            if ((i + 1) % (ptNum / 100) == 0) {
                // System.out.println(String.format("Progress: %d%%", (i + 1)*100 / ptNum));
            }
        }
        writer.close();
        writer = null;
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run() throws IOException, WriteProcessException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        for (int i = 0; i < repetition; i ++) {
            TsFileGenerator generator = new TsFileGenerator();
            if (align)
                generator.gen(true);
            else
                generator.genNonalign(true);
            double avgSpd = (sensorNum * deviceNum * ptNum) / (generator.timeConsumption / 1000.0);
            double memUsage = generator.monitorThread.getMaxMemUsage() / (1024.0 * 1024.0);
            totAvgSpd += avgSpd;
            totMemUsage += memUsage;
            System.out.println(String.format("TsFile generation completed. avg speed : %fpt/s, max memory usage: %fMB",
                    avgSpd, memUsage));
            File file = new File(filePath);
            totFileSize += file.length() / (1024.0 * 1024.0);
            if (!keepFile) {
                file.delete();
            }
        }
        System.out.println(String.format("FileName: %s; DataType: %s; Encoding: %s; DeviceNum: %d; SensorNum: %d; PtPerCol: %d; Wave: %s", filePath, dataType, encoding, deviceNum, sensorNum, ptNum, wave));
        System.out.println(String.format("Total Avg speed : %fpt/s; Total max memory usage: %fMB; File size: %fMB", totAvgSpd / repetition, totMemUsage / repetition, totFileSize / repetition));
    }

    public static void main(String[] args) throws IOException, WriteProcessException, JoranException {
        load("src/main/resources/logback.xml");
        filePath = "expr2.ts";
//        for(boolean ali: new boolean[]{true,false}){
//            align = ali;
//            for(int device : new int[]{1, 100, 1000, 10000, 100000}){
//                deviceNum = device;
//                for(int sensor : new int[] {1, 100, 1000, 10000, 100000}){
//                    sensorNum = sensor;
//                    for(TSDataType type : new TSDataType[]{TSDataType.FLOAT, TSDataType.INT64, TSDataType.INT32, TSDataType.DOUBLE}){
//                        dataType = type;
//                        repetition = 10;
//                        for (int pNum : new int[]{1, 100, 1000, 10000, 100000}) {
//                            ptNum = pNum;
//                            System.out.println();
//                            run();
//                        }
//                    }
//                }
//            }
//        }
//        logger.info("begin");
        align = true;
        deviceNum = 500;
        sensorNum = 10;
        repetition = 1;
        keepFile = true;
        dataType = TSDataType.FLOAT;
        for (int pNum : new int[]{10000}) {
            ptNum = pNum;
            run();
        }
//        FileOutputStream outputStream=new FileOutputStream("test",true);
//        System.out.println(outputStream.getChannel().position());
//        outputStream.write("123".getBytes());
//        System.out.println(outputStream.getChannel().position());


    }


    /**
     * 加载外部的logback配置文件
     *
     * @param externalConfigFileLocation
     *            配置文件路径
     * @throws IOException
     * @throws JoranException
     */
    public static void load(String externalConfigFileLocation) throws IOException, JoranException {

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();

        File externalConfigFile = new File(externalConfigFileLocation);

        if (!externalConfigFile.exists()) {

            throw new IOException("Logback External Config File Parameter does not reference a file that exists");

        } else {

            if (!externalConfigFile.isFile()) {
                throw new IOException("Logback External Config File Parameter exists, but does not reference a file");

            } else {

                if (!externalConfigFile.canRead()) {
                    throw new IOException("Logback External Config File exists and is a file, but cannot be read.");

                } else {

                    JoranConfigurator configurator = new JoranConfigurator();
                    configurator.setContext(lc);
                    lc.reset();
                    configurator.doConfigure(externalConfigFileLocation);

                    StatusPrinter.printInCaseOfErrorsOrWarnings(lc);
                }

            }

        }

    }
}