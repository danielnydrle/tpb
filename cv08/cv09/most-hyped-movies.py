# vytvorte v pyflinku skript, ktery najde 10 filmu s nejlepsimi hodnocenimi (5*)
# data jsou z MovieLens datasetu
# k dispozici mate soubory u.data (hodnoceni) a u-mod.item (filmy)
# vysledny seznam vratte setrideny podle poctu nejlepsich hodnoceni
# pocet take vypisujte
# ze souboru u-mod.items zjistete k nalezenym filmum prava jmena
# k finalni tabulce pridejte sloupec pocitajici pomer nejlepsich hodnoceni daneho filmu vuci vsem hodnocenim daneho filmu
# sestupne setridte

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, DataTypes, Schema, FormatDescriptor

# Nastavení prostředí
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# Cesty k souborům
ratings_path = "/files/cv09/u.data"
movies_path = "/files/cv09/u-mod.item"

# # Načtení hodnocení (u.data)
ratings_descriptor = TableDescriptor.for_connector("filesystem") \
    .schema(Schema.new_builder()
            .column("userID", DataTypes.INT())
            .column("movieID", DataTypes.INT())
            .column("rating", DataTypes.INT())
            .column("timestamp", DataTypes.STRING())
            .build()) \
    .option("path", ratings_path) \
    .format(FormatDescriptor.for_format("csv")
            .option("field-delimiter", "\t")
            .build()) \
    .build()
table_env.create_temporary_table("ratings", ratings_descriptor)
ratings = table_env.from_path("ratings")

movies_descriptor = TableDescriptor.for_connector("filesystem") \
    .schema(Schema.new_builder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .build()) \
    .option("path", movies_path) \
    .format(FormatDescriptor.for_format("csv")
            .option("field-delimiter", "|")
            .build()) \
    .build()
table_env.create_temporary_table("movies", movies_descriptor)
movies = table_env.from_path("movies")

grouped = ratings.group_by(ratings.movieID, ratings.rating).select(ratings.movieID, ratings.rating, ratings.rating.count.alias("count_o"))
sorted = grouped.order_by("count_o.desc").fetch(10)
joined = sorted.join(movies) \
    .where(sorted.movieID == movies.id) \
    .select(sorted.movieID, movies.name, sorted.count_o) \
    .order_by(sorted.count_o.desc)
joined.execute().print()
