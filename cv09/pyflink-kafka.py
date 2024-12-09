from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes, Schema, FormatDescriptor, TableDescriptor
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udf
from pyflink.table.window import Slide
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic

env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings, stream_execution_environment=env)

table_env.get_config().set_local_timezone("UTC")

# nutne kvuli zpracovani casu
table_env.execute_sql("""
        CREATE TABLE kafka_source (
                `title` STRING,
                `text` STRING,
                `comment_count` INT,
                `time` STRING,
                `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
                WATERMARK FOR `event_time` AS `event_time` - INTERVAL '1' SECOND
        ) WITH (
                'connector' = 'kafka',
                'topic' = 'test-topic',
                'properties.bootstrap.servers' = 'kafka:9092',
                'properties.group.id' = 'flink-group',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json'
        )
""")

table_env.create_temporary_table(
        "print_sink",
        TableDescriptor.for_connector("print")
                .schema(Schema.new_builder()
                        .column("title", DataTypes.STRING())
                        .column("event_time", DataTypes.TIMESTAMP(3))
                        .build())
                .build())

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

# table_env.create_temporary_table(
#         "unsequenced_sink",
#         TableDescriptor.for_connector("filesystem")
#                 .schema(Schema.new_builder()
#                         .column("title", DataTypes.STRING())
#                         .column("time", DataTypes.STRING())
#                         .column("previous_time", DataTypes.STRING())
#                         .build())
#                 .option("path", "/files/output/out_of_order")
#                 .format(FormatDescriptor.for_format("csv")
#                         .option("field-delimiter", ";")
#                         .build())
#                 .build())

data = table_env.from_path("kafka_source").select(col("title"), col("text"), col("comment_count"), col("time"), col("event_time"))
active_data = data \
        .filter(data.comment_count > 1) \
        .select(col("title"), col("comment_count"))
print_data = data.select(col("title"), col("event_time"))

# @udf(result_type=DataTypes.BOOLEAN())
# def is_violation(time, previous_time):
#     """Porovnává časy jako řetězce"""
#     return time > previous_time

# with_previous_time = with_previous_time = table_env.from_path("kafka_source") \
#     .select(
#         col("title"),
#         col("time"),
#         col("time").lag(1).over(
#             Tumble.over(lit(1).hours).on(col("event_time")).alias("previous_time")
#         )
#     )
# unsequenced_data = with_previous_time \
#     .filter(is_violation(col("time"), col("previous_time")))
# unsequenced_data.execute_insert("unsequenced_sink")


# data zpracovavejte posuvnym oknem nad vami pridanym casem
# delka 1 minuta s posunem 10 sekund
# pocitejte celkovy pocet clanku pridanych v danem okne a kolik z nich obsahuje v textu clanku vyraz "válka"
# zapisujte do souboru /files/output/windowed ve formatu zacatek_okna;konec_okna;pocet_clanku;clanky_s_valkou

windowed_data = data \
    .filter(col("event_time").is_not_null) \
    .window(Slide.over(lit(20).seconds).every(lit(2).seconds).on(col("event_time")).alias("w")) \
    .group_by(col("w")) \
    .select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("title").count.alias("total_articles"),
        col("text").like("%válka%").count.cast(DataTypes.BIGINT()).alias("articles_with_war")
    )

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

pipeline = table_env.create_statement_set()
# pipeline.add_insert("print_sink", print_data)
pipeline.add_insert("active_sink", active_data)
# pipeline.add_insert("unsequenced_sink", unsequenced_data)
pipeline.add_insert("windowed_sink", windowed_data)

pipeline.execute()

print_data.execute().print()

