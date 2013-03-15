package importAvro;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
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
    private static String rowKeyName;
    private static String columnFamily;

    private static DataFileReader<GenericRecord> read(Path filename) throws IOException
    {
        Configuration conf = new Configuration();
        FsInput fsInput = new FsInput(filename, conf);
        DatumReader<GenericRecord> datumReader =
                new GenericDatumReader<GenericRecord>();
        return new DataFileReader<GenericRecord>(fsInput, datumReader);
    }

    public static Schema discoverSchema(String inputPath) throws IOException
    {
        String filename = "part-m-00000.avro";
        Path outputFile = new Path(inputPath, filename);
        DataFileReader<GenericRecord> reader = read(outputFile);

        return reader.getSchema();
    }

    public class AvroImporter extends Mapper <AvroWrapper<GenericRecord>, NullWritable, ImmutableBytesWritable, KeyValue> {
        @Override
        protected void map(AvroWrapper<GenericRecord> key, NullWritable value, Context context)
                throws IOException, InterruptedException
        {
            GenericRecord record = key.datum();
            Schema schema = record.getSchema();
            List<Schema.Field> fields = schema.getFields();
            ArrayList<byte[]> columns = new ArrayList<byte[]>();
            for (Schema.Field f : fields)
            {
                columns.add(Bytes.toBytes(f.name()));
            }
            byte [] rawRowKey = Bytes.toBytes(record.get(rowKeyName).toString());
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

    private static Job createSubmitableJob(Configuration conf) throws IOException
    {

        JobConf jobConf = new JobConf(conf);

        jobConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, conf.get("importavro.hbase.zookeeper.quorum"));
        jobConf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, conf.get("importavro.hbase.zookeeper.property.clientPort"));
        jobConf.set("hbase.table.name", tableName);

        System.out.println("Hbase loc1 - " + jobConf.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM));
        jobConf.setJobName("importAvro" + "-" + tableName);
        jobConf.setInputFormat(AvroInputFormat.class);
        AvroJob.setInputSchema(jobConf, schema);

        System.out.println("Hbase loc2 - " + jobConf.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM));
        Job job = new Job(jobConf);
        FileInputFormat.setInputPaths(job, new Path(conf.get("importavro.inputPath")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("importavro.outputPath")));

        job.setMapperClass(AvroImporter.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        System.out.println("Hbase loc3 - " + job.getConfiguration().get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM));
        HTable hTable = new HTable(job.getConfiguration(), tableName);
        HFileOutputFormat.configureIncrementalLoad(job, hTable);

        return job;
    }


    /* -Dimportavro.inputPath = path
     * -Dimportavro.outputPath = path
     * -Dimportavro.columnf = <colf>
     * -Dimportavro.rowkey = <col>
     * -Dimportavro.tableName = tableName
     * -Dimportavro.hbase.zookeeper.quorum =
     * -Dimportavro.hbase.zookeeper.property.clientPort =
     * -Dimportavro.upload=<1/0> if 1 upload
     */
    public static void main(String[] args) throws Exception {
        System.out.println(">> ImportAvro");
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //Validate & initialize
        if(conf.get("importavro.inputPath") == null)
        {
            System.out.println("Need to specify input path of avro files");
            System.exit(-1);
        }
        if(conf.get("importavro.hbase.zookeeper.quorum") == null)
        {
            System.out.println("Need to specify hbase.zookeeper.quorum");
            System.exit(-1);
        }
        if(conf.get("importavro.hbase.zookeeper.property.clientPort") == null)
        {
            conf.set("importavro.hbase.zookeeper.property.clientPort", "2181");
        }
        if(conf.get("importavro.columnf") == null)
        {
            System.out.println("Need to specify importavro.columnf");
            System.exit(-1);
        }
        if(conf.get("importavro.rowkey") == null)
        {
            System.out.println("Need to specify importavro.rowkey");
            System.exit(-1);
        }
        if(conf.get("importavro.tableName") == null)
        {
            System.out.println("Need to specify importavro.tableName");
            System.exit(-1);
        }
        if(conf.get("importavro.outputPath") == null)
        {
                System.out.println("Need to specify importavro.outputPath");
                System.exit(-1);
        }
        if(conf.get("importavro.upload") == null)
        {
                System.out.println("Need to specify importavro.upload");
                System.exit(-1);
        }

        schema = discoverSchema(conf.get("importavro.inputPath"));
        tableName = conf.get("importavro.tableName");
        rowKeyName = conf.get("importavro.rowkey");
        columnFamily = conf.get("importavro.columnf");

        System.out.println("hbase-location1 : " + conf.get("importavro.hbase.zookeeper.quorum"));
        System.out.println("hbase-location2 : " + conf.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM));

        Job job = createSubmitableJob(conf);

        System.out.println("hbase-location3 : " + job.getConfiguration().get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM));
        HBaseAdmin hBaseAdmin = new HBaseAdmin(job.getConfiguration());
        if(!hBaseAdmin.tableExists(tableName))
        {
            System.out.println("Hbase table " + tableName + " does not exist");
            System.exit(-1);
        }

        job.waitForCompletion(true);

        if(conf.get("importavro.upload").equals("1"))
        {
            HTable hTable = new HTable(job.getConfiguration(), tableName);
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(job.getConfiguration());
            loader.doBulkLoad(new Path(conf.get("importavro.outputPath")), hTable);
        }
    }
}