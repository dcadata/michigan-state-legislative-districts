import os.path

import geopandas as gpd
import pandas as pd

_SHAPEFILES_DIR = 'G:/election_data/MichiganShapefiles/'


def _calculate_partial_intersection(precinct_geometry, district_geometry) -> float:
    intersection = precinct_geometry.intersection(district_geometry)
    intersection_area_pct = intersection.area / precinct_geometry.area
    intersection_area_cutoff = 0.01

    if intersection_area_pct > (1.0 - intersection_area_cutoff):  # fully* - slight incongruence, not real difference
        return 1.0
    elif intersection_area_pct > intersection_area_cutoff:
        return intersection_area_pct
    else:  # precinct and district do not intersect/have no overlap
        return 0.0


def _calculate_intersections(districts: gpd.GeoDataFrame, precincts: gpd.GeoDataFrame) -> pd.DataFrame:
    intersections = []
    keep_cols = ('PRECINCTID', 'COUNTYFIPS', 'MCDFIPS', 'WARD', 'PRECINCT')

    for precinct_row in precincts.to_dict('records'):
        precinct_geometry = precinct_row['geometry']
        if not precinct_geometry.is_valid:
            continue

        for district_row in districts.to_dict('records'):
            district_geometry = district_row['geometry']
            if precinct_geometry.within(district_geometry):
                intersections.append(dict(
                    **dict((col, precinct_row[col]) for col in keep_cols),
                    DISTRICTNO=district_row['DISTRICTNO'],
                    intersection=1,
                ))
                break
            elif intersection := _calculate_partial_intersection(precinct_geometry, district_geometry):
                intersections.append(dict(
                    **dict((col, precinct_row[col]) for col in keep_cols),
                    DISTRICTNO=district_row['DISTRICTNO'],
                    intersection=intersection,
                ))

    return pd.DataFrame(intersections).drop_duplicates()


def read_districts(chamber: str) -> gpd.GeoDataFrame:
    filename = dict(
        senate='StateSenate-FinalPlanLinden',
        house='StateHouse-FinalPlanHickory',
        congressional='Congressional-FinalPlanChestnut',
    )[chamber]
    districts = gpd.read_file(f'{_SHAPEFILES_DIR}{filename}.zip')[['DISTRICTNO', 'geometry']]
    districts.DISTRICTNO = districts.DISTRICTNO.apply(int)
    return districts


def read_precincts(voting_precincts_year: int) -> gpd.GeoDataFrame:
    if voting_precincts_year == 2016:
        voting_precincts_year = 2018
    precincts = gpd.read_file(f'{_SHAPEFILES_DIR}VotingPrecincts{voting_precincts_year}.zip')
    unused_cols = (
        'OBJECTID', 'OBJECTID_1', 'VP', 'PrecinctLa', 'ElectionYe', 'ELECTIONYE', 'ShapeSTAre', 'ShapeSTLen',
        'Shape_STAr', 'Shape_STLe',
    )
    cols_to_drop = [col for col in unused_cols if col in precincts.columns]
    precincts = precincts.drop(columns=cols_to_drop).rename(columns={
        'CountyFips': 'COUNTYFIPS',
        'Jurisdicti': 'MCDFIPS',
        'Ward': 'WARD',
        'Precinct': 'PRECINCT',
    })
    if 'PRECINCTID' not in precincts.columns:
        precincts['PRECINCTID'] = [
            'WP-{COUNTYFIPS}-{MCDFIPS}-{WARD}{PRECINCT}'.format(**record)
            for record in precincts[['COUNTYFIPS', 'MCDFIPS', 'WARD', 'PRECINCT']].to_dict('records')
        ]
    return precincts


def calculate_intersections_and_identify_missing_precincts(voting_precincts_year: int, chamber: str) -> None:
    districts = read_districts(chamber)
    precincts = read_precincts(voting_precincts_year)
    prefix = f'intersections/{voting_precincts_year}_{chamber}_'

    intersections = _calculate_intersections(districts, precincts)
    intersections.to_csv(f'{prefix}intersections.csv', index=False)

    missing_precincts = precincts.loc[~precincts.PRECINCTID.isin(intersections.PRECINCTID), [
        'PRECINCTID', 'COUNTYFIPS', 'MCDFIPS', 'WARD', 'PRECINCT']]
    missing_precincts.to_csv(f'{prefix}missing_precincts.csv', index=False)


def read_intersections(voting_precincts_year: int, chamber: str) -> pd.DataFrame:
    intersections = pd.read_csv(
        f'intersections/{voting_precincts_year}_{chamber}_intersections.csv',
        dtype={
            'PRECINCTID': str, 'COUNTYFIPS': str, 'MCDFIPS': str, 'WARD': str, 'PRECINCT': str,
            'DISTRICTNO': int, 'intersection': float,
        },
    )
    return intersections


def calculate_all_available_intersections() -> None:
    for year in (2014, 2016, 2018, 2020):
        for chamber in ('senate', 'house', 'congressional'):
            fp = f'intersections/{year}_{chamber}_intersections.csv'
            if os.path.exists(fp):
                continue
            print(fp)
            calculate_intersections_and_identify_missing_precincts(year, chamber)
