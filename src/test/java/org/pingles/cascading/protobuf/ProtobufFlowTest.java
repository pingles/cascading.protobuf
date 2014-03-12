package org.pingles.cascading.protobuf;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.*;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.test.HadoopPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@PlatformRunner.Platform({HadoopPlatform.class})

public class ProtobufFlowTest extends PlatformTestCase {
    private static final String TEST_DATA_ROOT = "./tmp/test";
    private static Map<Object, Object> properties = new HashMap<Object, Object>();
    private final JobConf conf = new JobConf();

    @Before
    public void setup() throws IOException {
        File outputDir = new File(TEST_DATA_ROOT);
        if (outputDir.exists()) {
            outputDir.delete();
        }
        FileUtils.forceMkdir(new File(TEST_DATA_ROOT));
    }

    @Test
    public void shouldKeepOnlyNames() throws IOException {
        String inputFile = "./tmp/test/data/small.seq";
        String outputDir = "./tmp/test/output/names-out";

        writePersonToSequenceFile(this.personBuilder().setId(123).setName("Paul").setEmail("test@pingles.org").build(), inputFile);

        Tap source = getPlatform().
        		getTap(new ProtobufSequenceFileScheme(Messages.Person.class, new Fields("id", "name", "email")), 
        				inputFile,
        				SinkMode.KEEP);
        Tap sink = getPlatform().
        		getTextFile(null, null, outputDir, SinkMode.REPLACE);
        Pipe pipe = new Each("Extract names", new Fields("name"), new Identity());

        Flow flow = getPlatform().getFlowConnector().connect(source, sink, pipe);
        flow.complete();

        List<String> lines = FileUtils.readLines(new File(outputDir + "/part-00000"));

        assertEquals(1, lines.size());
        assertEquals("Paul", lines.get(0));
    }

    @Test
    public void shouldConvertUninitalisedOptionalFieldsToNull() throws IOException {
        String inputFile = "./tmp/test/data/small.seq";
        String outputDir = "./tmp/test/output/names-out";

        Messages.Passport passport = this.personBuilder().getPassportBuilder().setPassportNumber(12345).build();
        writePersonToSequenceFile(this.personBuilder()
                .setId(123)
                .setName("Paul")
                .build(), inputFile);

        Tap source = new Lfs(new ProtobufSequenceFileScheme(Messages.Person.class, Fields.ALL), inputFile);
        Tap sink = new Lfs(new TextLine(), outputDir, SinkMode.REPLACE);
        Pipe pipe = new Pipe("Pass through");

        Flow flow = getPlatform().getFlowConnector().connect(source, sink, pipe);
        flow.complete();

        List<String> lines = FileUtils.readLines(new File(outputDir + "/part-00000"));

        assertEquals(1, lines.size());
        // there's an empty tab because there are no friends... weird huh
        assertEquals("123\tPaul\t\t[]\t", lines.get(0));
    }
    
    @Test
    public void shouldWorkWithSpecifyingFieldsALL() throws IOException {
        String inputFile = "./tmp/test/data/small.seq";
        String outputDir = "./tmp/test/output/names-out";


        Messages.Passport passport = this.personBuilder().getPassportBuilder().setPassportNumber(12345).build();
        writePersonToSequenceFile(this.personBuilder()
                .setId(123)
                .setName("Paul")
                .setEmail("test@pingles.org")
                .setPassport(passport)
                .build(), inputFile);

        Tap source = new Lfs(new ProtobufSequenceFileScheme(Messages.Person.class, Fields.ALL), inputFile);
        Tap sink = new Lfs(new TextLine(), outputDir, SinkMode.REPLACE);
        Pipe pipe = new Pipe("Pass through");

        Flow flow = getPlatform().getFlowConnector().connect(source, sink, pipe);
        flow.complete();

        List<String> lines = FileUtils.readLines(new File(outputDir + "/part-00000"));

        assertEquals(1, lines.size());
        // there's an empty tab because there are no friends... weird huh
        assertEquals("123\tPaul\ttest@pingles.org\t[]\t12345", lines.get(0));
    }

    @Test
    public void shouldKeepNamesAndEmail() throws IOException {
        String inputFile = "./tmp/test/data/small.seq";
        String outputDir = "./tmp/test/output/names-out";

        writePersonToSequenceFile(this.personBuilder().setId(123).setName("Paul").setEmail("test@pingles.org").build(), inputFile);

        Tap source = new Lfs(new ProtobufSequenceFileScheme(Messages.Person.class, new Fields("id", "name", "email")), inputFile);
        Tap sink = new Lfs(new TextLine(), outputDir, SinkMode.REPLACE);
        Pipe pipe = new Each("Extract names", new Fields("name", "email"), new Identity());

        Flow flow = getPlatform().getFlowConnector().connect(source, sink, pipe);
        flow.complete();

        List<String> lines = FileUtils.readLines(new File(outputDir + "/part-00000"));

        assertEquals(1, lines.size());
        assertEquals("Paul\ttest@pingles.org", lines.get(0));
    }

    @Test
    public void shouldSetEmailFieldToEmptyStringWhenNotSetOnMessage() throws IOException {
        String inputFile = "./tmp/test/data/small.seq";
        String outputDir = "./tmp/test/output/names-out";

        writePersonToSequenceFile(this.personBuilder().setId(123).setName("Paul").build(), inputFile);

        Tap source = new Lfs(new ProtobufSequenceFileScheme(Messages.Person.class, new Fields("id", "name", "email")), inputFile);
        Tap sink = new Lfs(new TextLine(), outputDir, SinkMode.REPLACE);
        Pipe pipe = new Each("Extract names", new Fields("name", "email"), new Identity());

        Flow flow = getPlatform().getFlowConnector().connect(source, sink, pipe);
        flow.complete();

        List<String> lines = FileUtils.readLines(new File(outputDir + "/part-00000"));

        assertEquals(1, lines.size());
        assertEquals("Paul\t", lines.get(0));
    }

    @Test
    public void shouldHandleRepeatedFields() throws IOException {
        String inputFile = "./tmp/test/data/small.seq";
        String outputDir = "./tmp/test/output/names-out";

        Messages.Person strongbad = this.personBuilder().setId(456).setName("strongbad").build();
        Messages.Person homestar = this.personBuilder().setId(789).setName("homestar").addFriends(strongbad).build();
        Messages.Person paul = this.personBuilder().setId(123).setName("Paul").addFriends(strongbad).addFriends(homestar).build();

        writePersonToSequenceFile(paul, inputFile);

        Tap source = new Lfs(new ProtobufSequenceFileScheme(Messages.Person.class, new Fields("id", "name", "email", "friends")), inputFile);
        Tap sink = new Lfs(new TextLine(), outputDir, SinkMode.REPLACE);
        Pipe pipe = new Each("Extract friends", new Fields("friends"), new Identity());
        pipe = new Each(pipe, new ExtractFriendsNames(new Fields("id", "name", "email", "friends", "passport"), new Fields("name")));

        Flow flow = getPlatform().getFlowConnector().connect(source, sink, pipe);
        flow.complete();

        List<String> lines = FileUtils.readLines(new File(outputDir + "/part-00000"));

        assertEquals(2, lines.size());
        assertEquals("strongbad", lines.get(0));
        assertEquals("homestar", lines.get(1));
    }

    private Messages.Person.Builder personBuilder() {
        return Messages.Person.newBuilder();
    }

    private void writePersonToSequenceFile(Messages.Person person, String path) throws IOException {
        SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.getLocal(conf), conf, new Path(path), LongWritable.class, BytesWritable.class);
        try {
            writer.append(new LongWritable(1), new BytesWritable(person.toByteArray()));
        } finally {
            writer.close();
        }
    }
}
