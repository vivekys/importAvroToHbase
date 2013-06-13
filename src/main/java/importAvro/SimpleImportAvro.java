package importAvro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class SimpleImportAvro {
    private static String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

    private static Schema schema;
    private static String tableName;
    private static String deleteOption;
    private static String createIfNotPresent;
    private static String columnf;

    private static DataFileReader<GenericRecord> read(Path filename) throws IOException {
        Configuration conf = new Configuration();
        FsInput fsInput = new FsInput(filename, conf);
        DatumReader<GenericRecord> datumReader =
                new GenericDatumReader<GenericRecord>();
        return new DataFileReader<GenericRecord>(fsInput, datumReader);
    }

    public static Schema discoverSchema(String inputPath) throws IOException {
        String filename = "part-m-00000.avro";
        Path outputFile = new Path(inputPath, filename);
        DataFileReader<GenericRecord> reader = read(outputFile);

        return reader.getSchema();
    }

    public static class AvroImporter extends Mapper<AvroKey<GenericData.Record>, NullWritable, ImmutableBytesWritable, KeyValue> {
        @Override
        protected void map(AvroKey<GenericData.Record> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            GenericRecord record = key.datum();
            Schema schema = record.getSchema();
            List<Schema.Field> fields = schema.getFields();
            ArrayList<byte[]> columns = new ArrayList<byte[]>();
            for (Schema.Field f : fields) {
                columns.add(Bytes.toBytes(f.name()));
                System.out.println(f.name());
            }
            String rowKeyName = context.getConfiguration().get("importavro.rowkey");
            String columnFamily = context.getConfiguration().get("importavro.columnf");

            byte[] rawRowKey = Bytes.toBytes(record.get(rowKeyName).toString());
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rawRowKey);
            for (int i = 0; i < fields.size(); i++) {
                Schema.Field field = fields.get(i);
                Object fieldValue = record.get(field.name());
                if (fieldValue != null) {
                    byte[] finalValue = Bytes.toBytes(fieldValue.toString());
                    KeyValue kv = new KeyValue(rowKey.copyBytes(), Bytes.toBytes(columnFamily), columns.get(i), finalValue);
                    context.write(rowKey, kv);
                }
            }
        }
    }

    private static Job createSubmitableJob(Configuration conf) throws IOException {
        conf.set("hbase.table.name", tableName);

        Job job = new Job(conf);
        job.setJarByClass(AvroImporter.class);

        FileInputFormat.setInputPaths(job, new Path(conf.get("importavro.inputPath")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("importavro.outputPath")));

        AvroJob.setInputKeySchema(job, schema);

        job.setJobName("importAvro" + "-" + tableName);

        job.setMapperClass(AvroImporter.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        HTable hTable = new HTable(job.getConfiguration(), tableName);
        HFileOutputFormat.configureIncrementalLoad(job, hTable);
        return job;
    }

    private static void deleteHbaseTable(HBaseAdmin hBaseAdmin, String tableName) throws IOException {
        if (!hBaseAdmin.tableExists(tableName)) {
            System.out.println(tableName + "does not exist");
            System.exit(-1);
        }

        hBaseAdmin.disableTable(tableName);
        hBaseAdmin.deleteTable(tableName);
    }

    /* -Dimportavro.inputPath = path
     * -Dimportavro.outputPath = path
     * -Dimportavro.columnf = <colf>
     * -Dimportavro.rowkey = <col>
     * -Dimportavro.tableName = tableName
     * -Dimportavro.hbase.zookeeper.quorum =
     * -Dimportavro.hbase.zookeeper.property.clientPort =
     * -Dimportavro.upload = <1/0> if 1 upload
     * -Dimportavro.createIfNotPresent = <1/0> if 1 create hbase table if not present
     * -Dimportavro.deleteOption = <1/0> if 1 delete hbase table
     */
    public static void main(String[] args) throws Exception {
        System.out.println(">> ImportAvro");
        Configuration conf = new Configuration();
        @SuppressWarnings("unused")
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //Validate & initialize
        if (conf.get("importavro.inputPath") == null) {
            System.out.println("Need to specify input path of avro files");
            System.exit(-1);
        }
        if (conf.get("importavro.hbase.zookeeper.quorum") == null) {
            System.out.println("Need to specify hbase.zookeeper.quorum");
            System.exit(-1);
        }
        if (conf.get("importavro.hbase.zookeeper.property.clientPort") == null) {
            conf.set("importavro.hbase.zookeeper.property.clientPort", "2181");
        }
        if (conf.get("importavro.columnf") == null) {
            System.out.println("Need to specify importavro.columnf");
            System.exit(-1);
        }
        if (conf.get("importavro.rowkey") == null) {
            System.out.println("Need to specify importavro.rowkey");
            System.exit(-1);
        }
        if (conf.get("importavro.tableName") == null) {
            System.out.println("Need to specify importavro.tableName");
            System.exit(-1);
        }
        if (conf.get("importavro.outputPath") == null) {
            conf.set("importavro.outputPath", "/tmp" + conf.get("importavro.inputPath"));
        }
        if (conf.get("importavro.upload") == null) {
            System.out.println("Need to specify importavro.upload");
            System.exit(-1);
        }

        conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, conf.get("importavro.hbase.zookeeper.quorum"));
        conf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, conf.get("importavro.hbase.zookeeper.property.clientPort"));
        tableName = conf.get("importavro.tableName");
        deleteOption = conf.get("importavro.deleteOption");
        createIfNotPresent = conf.get("importavro.createIfNotPresent");
        columnf = conf.get("importavro.columnf");

        schema = discoverSchema(conf.get("importavro.inputPath"));
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);

        if (deleteOption != null) {
            if (deleteOption.equals("1")) {
                deleteHbaseTable(hBaseAdmin, tableName);
                System.out.println("Deleted the hbase table--" + tableName);
                System.exit(-1);
            }
        }

        if (!hBaseAdmin.tableExists(tableName)) {
            System.out.println("Hbase table " + tableName + " does not exist");

            if (createIfNotPresent == null) {
                System.exit(-1);
            }
            if (createIfNotPresent.equals("1")) {
                HTableDescriptor htd = new HTableDescriptor(tableName);
                htd.addFamily(new HColumnDescriptor(columnf));
                hBaseAdmin.createTable(htd);
                System.out.println("Created the hbase table--" + tableName);
            } else {
                System.exit(-1);
            }
        }

        Job job = createSubmitableJob(conf);
        job.waitForCompletion(true);

        if (conf.get("importavro.upload").equals("1")) {
            HTable hTable = new HTable(job.getConfiguration(), tableName);
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(job.getConfiguration());
            loader.doBulkLoad(new Path(conf.get("importavro.outputPath")), hTable);
        }
    }
}
