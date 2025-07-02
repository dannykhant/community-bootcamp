from chispa.dataframe_comparer import assert_df_equality

from ..jobs.team_vertex_job import do_team_vertex_transformation
from collections import namedtuple

team = namedtuple(
    "Team",
    "team_id abbreviation nickname city arena yearfounded"
)

team_vertex = namedtuple(
    "TeamVertex",
    "identifier type properties"
)


def test_team_vertex(spark):
    input_data = [
        team(1, "GSW", "Warriors", "San Francisco", "Chase Center", 1900),
        team(1, "GSW", "Bad Warriors", "San Francisco", "Chase Center", 1900)
    ]
    input_df = spark.createDataFrame(input_data)
    actual_df = do_team_vertex_transformation(spark, input_df)

    expected_data = [
        team_vertex(
            identifier=1,
            type='team',
            properties={
                'abbreviation': 'GSW',
                'nickname': 'Warriors',
                'city': 'San Francisco',
                'arena': 'Chase Center',
                'year_founded': '1900'
            }
        )
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
