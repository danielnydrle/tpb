from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Schema, FormatDescriptor, TableDescriptor
from pyflink.table.expressions import col

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

table_env.create_temporary_table(
    'kafka_source',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column("title", DataTypes.STRING())
                .column("comment_count", DataTypes.INT())
                .column("event_time", DataTypes.TIMESTAMP(3))
                .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                .build())
        .option('topic', 'test-topic')
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('properties.group.id', 'flink-group')
        .option('scan.startup.mode', 'latest-offset')
        .format(FormatDescriptor.for_format('json')
                .build())
        .build())

table_env.create_temporary_table(
        'print_sink',
        TableDescriptor.for_connector("print")
                .schema(Schema.new_builder()
                        .column('title', DataTypes.STRING())
                        .build())
                .build())

table_env.create_temporary_table(
        'file_sink',
        TableDescriptor.for_connector("filesystem")
                .schema(Schema.new_builder()
                        .column('title', DataTypes.STRING())
                        .column('comment_count', DataTypes.INT())
                        .build())
                .option('path', '/files/output/table')
                .format(FormatDescriptor.for_format('csv')
                        .option("field-delimiter", ";")
                        .build())
                .build())

active_data = table_env.from_path("kafka_source").filter(table_env.from_path("kafka_source").comment_count > 1).drop_columns(col('event_time'))
active_data.execute_insert('file_sink')

print_data = table_env.from_path("kafka_source").drop_columns(col('comment_count'), col('event_time'))
print_data.execute_insert("print_sink")

print_data.execute().print()
