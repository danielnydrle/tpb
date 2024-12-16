from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes, Schema, FormatDescriptor, TableDescriptor, Row
from pyflink.table.expressions import col, lit
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table.window import Slide, Tumble
from pyflink.table.udf import udf, ScalarFunction

env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = StreamTableEnvironment.create(
    environment_settings=env_settings, stream_execution_environment=env)

table_env.get_config().set_local_timezone("UTC")

table_env.create_temporary_table(
    'kafka_source',
    TableDescriptor.for_connector('kafka')
    .schema(Schema.new_builder()
            .column('value', DataTypes.STRING())
            .column_by_expression('event_time', 'PROCTIME()')
            .build())
    .option('topic', 'test-topic')
    .option('properties.bootstrap.servers', 'kafka:9092')
    .option('properties.group.id', 'flink-group')
    .option('scan.startup.mode', 'latest-offset')
    .format(FormatDescriptor.for_format('raw')
            .build())
    .build())

data = table_env.from_path("kafka_source")

table = data.select(col("value").json_value("$.title", DataTypes.STRING()).alias("title"),
                    col("value").json_value(
                        "$.text", DataTypes.STRING()).alias("text"),
                    col("value").json_value("$.comment_count", DataTypes.INT()).cast(
                        DataTypes.INT()).alias("comment_count"),
                    col("event_time"))

# print
table_env.create_temporary_table(
    "print_sink",
    TableDescriptor.for_connector("print")
    .schema(Schema.new_builder()
            .column("title", DataTypes.STRING())
            .column("event_time", DataTypes.TIMESTAMP(3))
            .build())
    .build())
print_table = table.select(col("title"), col("event_time"))


# active articles
table_env.create_temporary_table(
    "active_sink",
    TableDescriptor.for_connector("filesystem")
    .schema(Schema.new_builder()
            .column("title", DataTypes.STRING())
            .column("comment_count", DataTypes.INT())
            .build())
    .option("path", "/files/output/active")
    .format(FormatDescriptor.for_format("csv")
            .option("field-delimiter", ";")
            .build())
    .build())
active_table = table \
    .filter(col("comment_count") > 1) \
    .select(col("title"), col("comment_count"))

# unsequenced articles
table = table.add_columns(
    col("event_time").cast(DataTypes.DATE()).alias("event_time_ts")
)


class UpdateSequence(ScalarFunction):
    def __init__(self):
        self.last_time = None

    def eval(self, event_time_ts):
        prev_time = self.last_time
        self.last_time = event_time_ts
        return prev_time


update_sequence = udf(UpdateSequence(), result_type=DataTypes.DATE())

table = table.add_columns(update_sequence(
    col("event_time_ts")).alias("prev_time"))

table_env.create_temporary_table(
    "unsequenced_sink",
    TableDescriptor.for_connector("filesystem")
    .schema(Schema.new_builder()
            .column("title", DataTypes.STRING())
            .column("event_time_ts", DataTypes.DATE())
            .column("event_time", DataTypes.DATE())
            .build())
    .option("path", "/files/output/unsequenced")
    .format(FormatDescriptor.for_format("csv")
            .option("field-delimiter", ";")
            .build())
    .build())
unsequenced_table = table \
    .select(col("title"), col("event_time_ts"), update_sequence(col("event_time_ts")).alias("event_time")) \
    .filter(col("event_time") < col("event_time_ts"))

# war articles
table_env.create_temporary_table(
    "windowed_sink",
    TableDescriptor.for_connector("filesystem")
    .schema(Schema.new_builder()
            .column("window_start", DataTypes.TIMESTAMP(3))
            .column("window_end", DataTypes.TIMESTAMP(3))
            .column("total_articles", DataTypes.BIGINT())
            .column("articles_with_war", DataTypes.BIGINT())
            .build())
    .option("path", "/files/output/windowed")
    .format(FormatDescriptor.for_format("csv")
            .option("field-delimiter", ";")
            .build())
    .build())
windowed_table = table \
    .window(Slide.over(lit(1).minutes).every(lit(10).seconds).on(col("event_time")).alias("w")) \
    .group_by(col("w")) \
    .select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("title").count.alias("total_articles"),
        col("text").like("% a %").cast(
            DataTypes.INT()).sum.alias("articles_with_war")
    )

pipeline = table_env.create_statement_set()
pipeline.add_insert("print_sink", print_table)
pipeline.add_insert("active_sink", active_table)
pipeline.add_insert("unsequenced_sink", unsequenced_table)
pipeline.add_insert("windowed_sink", windowed_table)

pipeline.execute().wait()
