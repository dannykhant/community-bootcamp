from chispa.dataframe_comparer import assert_df_equality

from ..jobs.players_scd_job import do_players_scd
from collections import namedtuple

player_season = namedtuple(
    "PlayerSeason",
    "player_name current_season scoring_class"
)

player_scd = namedtuple(
    "PlayerScd",
    "player_name scoring_class start_date end_date"
)


def test_players_scd(spark):
    source_data = [
        player_season("Michael Jordan", 2001, 'Good'),
        player_season("Michael Jordan", 2002, 'Good'),
        player_season("Michael Jordan", 2003, 'Bad'),
        player_season("Someone Else", 2003, 'Bad')
    ]
    source_df = spark.createDataFrame(source_data)
    actual_df = do_players_scd(spark, source_df)

    expected_data = [
        player_scd("Michael Jordan", 'Good', 2001, 2002),
        player_scd("Michael Jordan", 'Bad', 2003, 2003),
        player_scd("Someone Else", 'Bad', 2003, 2003)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)
