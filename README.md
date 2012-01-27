# cascading.protobuf

Provides a `ProtobufSequenceFileScheme` suitable for consuming [Protocol Buffer](http://code.google.com/apis/protocolbuffers/)
encoded messages. Currently only supports operation as a source.

## Usage

The scheme assumes that the input to your source is a `SequenceFile<LongWritable, BytesWritable>` where the
value contains the raw serialized bytes of your message.

Because Protocol Buffers are schema-based, when constructing the scheme it's also necessary to provide the
generated Java class for your message:

```java
String inputPath = "./input.seq";
Scheme personScheme = new ProtobufSequenceFileScheme(Messages.Person.class, new Fields("id", "name", "email"))
Tap source = new Lfs(scheme, inputPath);
```