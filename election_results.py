import geopandas as gpd
import pandas as pd

import shapes

_OFFICE_NAMES = dict(
    senate='STATE SENATOR',
    house='REPRESENTATIVE IN STATE LEG',
    congressional='REPRESENTATIVE IN CONGRESS',
)


def read_primary_data(year: int) -> pd.DataFrame:
    filepath = f'G:/election_data/MichiganElectionResults/Primary/{year}.xls'
    dtype = {
        'ElectionDate': str,
        'OfficeCode(Text)': str,
        'DistrictCode(Text)': str,
        'CountyCode': str,
        'CountyName': str,
        'OfficeDescription': str,
        'PartyName': str,
        'CandidateID': str,
        'CandidateLastName': str,
        'CandidateFirstName': str,
        # 'CandidateMiddleName': str,
        # 'CandidateFormerName': str,
        'CandidateVotes': float,
    }
    rename_mapper = {
        'OfficeCode(Text)': 'OfficeCode',
        'DistrictCode(Text)': 'DistrictCode',
    }
    data = pd.read_csv(filepath, sep='\t', dtype=dtype)[list(dtype.keys())]
    data.CandidateVotes = data.CandidateVotes.fillna(0).apply(int)
    data['CandidateName'] = data.CandidateLastName + ', ' + data.CandidateFirstName
    data = data.iloc[:-1].dropna()
    data = data.rename(columns=rename_mapper).drop(columns=['CandidateLastName', 'CandidateFirstName'])
    return data


def _read_file(filename: str, year: int, dtype: dict) -> pd.DataFrame:
    filepath = f'G:/election_data/MichiganElectionResults/General/{year}/{filename}'
    data = pd.read_csv(filepath, sep='\t', index_col=False, names=dtype.keys(), dtype=dtype)
    keep_cols = [col for col in dtype.keys() if col[0] != 'x']
    data = data[keep_cols].copy()
    return data


def _normalize_mcd_name(x: str) -> str:
    x = x.strip()
    abbreviations = {
        'ST.': 'ST',
        'STE.': 'STE',
        'SAINT': 'ST',
        'MT.': 'MT',
        'MOUNT': 'MT',
    }
    for key, value in abbreviations.items():
        x = x.replace(key, value)
    return x


def _read_offices(year: int) -> pd.DataFrame:
    dtype = {
        'x election_year': str,
        'x election_type': str,
        'office_code': int,
        'district_code': str,
        'status_code': int,
        'office_desc': str,
    }
    offices = _read_file(f'{year}offc.txt', year, dtype)
    return offices


def _read_parties(year: int) -> pd.DataFrame:
    dtype = {
        'x election_year': str,
        'x election_type': str,
        'office_code': int,
        'x district_code': str,
        'x status_code': int,
        'candidate_id': int,
        'candidate_last_name': str,
        'candidate_first_name': str,
        'x candidate_middle_name': str,
        'party': str,
    }
    parties = _read_file(f'{year}name.txt', year, dtype)
    parties.party = parties.party.apply(lambda x: x if x in {'DEM', 'REP'} else 'OTH')  # normalize party names
    parties['cand'] = parties.candidate_last_name + ', ' + parties.candidate_first_name
    parties = parties.drop(columns=['candidate_last_name', 'candidate_first_name'])
    return parties


def _read_votes(year: int) -> pd.DataFrame:
    dtype = {
        'x election_year': str,
        'x election_type': str,
        'office_code': int,
        'x district_code': str,
        'x status_code': int,
        'candidate_id': int,
        'county_code': int,
        'mcd_code': int,  # 'city/town_code',
        'WARD': str,
        'PRECINCT': str,
        'x precinct_label': str,
        'votes': str,  # Has unexpected 'NA' that needs to be dropped before converting to int
    }
    votes = _read_file(f'{year}vote.txt', year, dtype)
    votes.votes = votes.votes.replace('NA', '0').fillna('0').apply(int)
    votes.WARD = votes.WARD.apply(lambda x: x.zfill(2))
    votes.PRECINCT = votes.PRECINCT.apply(lambda x: x.zfill(3))
    return votes


def _read_mcd(year: int) -> pd.DataFrame:
    dtype = {
        'x election_year': str,
        'x election_type': str,
        'county_code': int,
        'mcd_code': int,
        'mcd_name': str,
    }
    mcd = _read_file(f'{year}city.txt', year, dtype)
    mcd = mcd[mcd.mcd_code != 9999].copy()
    return mcd


def _read_counties(year: int) -> pd.DataFrame:
    dtype = {
        'county_code': int,
        'county_name': str,
    }
    counties = _read_file('county.txt', year, dtype)
    return counties


def _read_mcd_fips_mapper() -> pd.DataFrame:
    mcd_fips_mapper = gpd.read_file('G:/election_data/MichiganShapefiles/MinorCivilDivisions.zip').iloc[:, 1:6].drop(
        columns='FIPSNUM')
    for col in ('LABEL', 'NAME', 'TYPE'):
        mcd_fips_mapper[col] = mcd_fips_mapper[col].apply(lambda x: x.upper())
    mcd_fips_mapper['NAME_TYPE'] = mcd_fips_mapper.NAME + ' ' + mcd_fips_mapper.TYPE
    return mcd_fips_mapper


def _get_office_codes(offices: pd.DataFrame, office_name: str, one: bool = False) -> [list, pd.DataFrame]:
    df = offices.loc[offices.office_desc.str.contains(office_name.upper()), ['office_code', 'office_desc']]
    if one:
        return df.office_code.values[0]  # list
    else:
        return df  # dataframe


def _merge_election_results(
        offices: pd.DataFrame,
        office_name: str,
        parties: pd.DataFrame,
        votes: pd.DataFrame,
        mcd: pd.DataFrame,
        counties: pd.DataFrame,
) -> pd.DataFrame:
    office_code = _get_office_codes(offices, office_name, one=True)
    parties = parties[parties.office_code == office_code].drop(columns='office_code')
    votes = votes[votes.office_code == office_code].drop(columns='office_code')

    votes_merged = (
        votes
            .merge(parties, on='candidate_id')
            .merge(mcd, on=['mcd_code', 'county_code'])
            .merge(counties, on='county_code')
            .drop(columns=['county_code', 'mcd_code', 'candidate_id'])
    )
    return votes_merged


def read_and_merge_election_results(year: int, office_name: str) -> pd.DataFrame:
    offices = _read_offices(year)
    parties = _read_parties(year)
    votes = _read_votes(year)
    mcd = _read_mcd(year)
    counties = _read_counties(year)
    election_results = _merge_election_results(offices, office_name, parties, votes, mcd, counties)
    return election_results


def _transpose_parties_into_columns(votes_rollup: pd.DataFrame) -> pd.DataFrame:
    merge_cols = ['county_name', 'mcd_name', 'FIPSCODE', 'WARD', 'PRECINCT']

    total_votes = votes_rollup.groupby(merge_cols, as_index=False).votes.sum().rename(columns=dict(votes='totalvot'))
    votes_grouped = votes_rollup.groupby(merge_cols + ['party'], as_index=False).votes.sum()

    def _separate_party(p: str) -> pd.DataFrame:
        return votes_grouped[votes_grouped.party == p].drop(columns='party').rename(columns={
            'votes': f'{p[0].lower()}vot'})

    votes_parties = _separate_party('DEM').merge(_separate_party('REP'), how='outer', on=merge_cols).merge(
        _separate_party('OTH'), how='outer', on=merge_cols)
    for party in ('d', 'r', 'o'):
        votes_parties[f'{party}vot'] = votes_parties[f'{party}vot'].fillna(0)
    votes_parties = votes_parties.merge(total_votes, on=merge_cols)
    return votes_parties


def _combine_election_results_with_mcd_fips(election_results: pd.DataFrame) -> pd.DataFrame:
    mcd_fips_mapper = _read_mcd_fips_mapper()
    election_results.mcd_name = election_results.mcd_name.apply(_normalize_mcd_name)
    election_results = pd.concat(election_results.merge(mcd_fips_mapper, left_on='mcd_name', right_on=col) for col in (
        'LABEL', 'NAME', 'NAME_TYPE'))
    election_results = election_results.drop(columns=['LABEL', 'NAME', 'TYPE', 'NAME_TYPE'])
    # drop_duplicates suddenly doesn't accept the subset argument?
    # noinspection PyArgumentList
    election_results = election_results.drop_duplicates(subset=['FIPSCODE', 'WARD', 'PRECINCT', 'party', 'cand'])
    # which is then messing up the type of election_results
    # noinspection PyTypeChecker
    election_results = _transpose_parties_into_columns(election_results)
    election_results = election_results.rename(columns={'FIPSCODE': 'MCDFIPS'})
    return election_results


def _add_voteshare_and_margin(election_results: pd.DataFrame) -> pd.DataFrame:
    total_vot = election_results.dvot.map(int) + election_results.rvot.map(int)
    election_results['voteShareD_2party'] = election_results.dvot.map(int) / total_vot
    election_results['voteShareR_2party'] = election_results.rvot.map(int) / total_vot
    election_results['margin_2party'] = (
            election_results.voteShareD_2party.map(float) - election_results.voteShareR_2party.map(float))
    for col in ('voteShareD_2party', 'voteShareR_2party', 'margin_2party'):
        election_results[col] = election_results[col].apply(lambda x: round(x, 3))
    election_results['marginText_2party'] = election_results.margin_2party.apply(
        lambda x: f'{"D" if x > 0 else "R"}+{round(abs(x) * 100, 1)}')
    election_results['winner'] = election_results.margin_2party.apply(lambda x: 'D' if x > 0 else 'R')
    election_results = election_results.drop(columns=['dvot', 'rvot', 'ovot', 'totalvot'])
    return election_results


def create_district_level_summary(
        year: int,
        office_name: str,
        chamber: str,
        save_data: bool = False,
        save_plot: bool = False,
        filename_label: str = None,
) -> gpd.GeoDataFrame:
    election_results = read_and_merge_election_results(year, office_name)
    df = _combine_election_results_with_mcd_fips(election_results)
    df = df.merge(shapes.read_intersections(year, chamber), on=['MCDFIPS', 'WARD', 'PRECINCT'])
    df = df.drop_duplicates(subset=['county_name', 'mcd_name', 'WARD', 'PRECINCT'])
    for col in ('dvot', 'rvot', 'ovot', 'totalvot'):
        df[col] = df[col] * df['intersection']
    df = df.groupby('DISTRICTNO', as_index=False).agg(dict(dvot=sum, rvot=sum, ovot=sum, totalvot=sum))
    df = df.merge(shapes.read_districts(chamber), on='DISTRICTNO')
    df = _add_voteshare_and_margin(df)
    df = gpd.GeoDataFrame(df.to_dict('records'))

    if save_data:
        df.drop(columns='geometry').to_csv(
            f'2022_districts/{filename_label} by {chamber[0].upper()}D {year}.csv', index=False)

    if save_plot:
        plt = df.plot('margin', cmap='RdYlBu', legend='margin', vmin=-0.5, vmax=0.5)
        plt.set_title(f'{year} {filename_label} Results by State {chamber.title()} District')

    return df


def create_county_level_election_results_summary(year: int, office_name: str) -> pd.DataFrame:
    df = read_and_merge_election_results(year, office_name)
    df = df.groupby(['county_name', 'party'], as_index=False).votes.sum()

    _separate_party = lambda p: df[df.party == p].drop(columns='party').rename(columns={'votes': f'votes{p[0]}'})
    df = _separate_party('DEM').merge(_separate_party('REP'), on='county_name').merge(
        _separate_party('OTH'), on='county_name')
    assert len(df) == 83

    total_votes = df.votesD + df.votesR + df.votesO
    for i in ('D', 'R', 'O'):
        df[f'voteShare{i}'] = ((df[f'votes{i}'] / total_votes) * 100).round(1)
    df['margin'] = (df.voteShareD - df.voteShareR).round(1)

    total_votes_2party = df.votesD + df.votesR
    for i in ('D', 'R'):
        df[f'voteShare{i}_2party'] = ((df[f'votes{i}'] / total_votes_2party) * 100).round(1)
    df['margin_2party'] = (df.voteShareD_2party - df.voteShareR_2party).round(1)

    df = df.rename(columns=dict(county_name='countyName'))
    return df


def create_benchmarks() -> None:
    df = create_county_level_election_results_summary(2018, 'governor')[[
        'countyName', 'voteShareD_2party', 'voteShareR_2party', 'margin_2party', 'votesD', 'votesR']]
    votesD_sum = df.votesD.sum()
    votesR_sum = df.votesR.sum()
    total_votes_2party = votesD_sum + votesR_sum  # 2-party vote share
    statewide_margin = ((votesD_sum / total_votes_2party - votesR_sum / total_votes_2party) * 100).round(1)
    df = df.drop(columns=['votesD', 'votesR'])

    prez2020 = create_county_level_election_results_summary(2020, 'president of the united states')[[
        'countyName', 'voteShareD_2party', 'voteShareR_2party', 'margin_2party']]
    df = df.merge(prez2020, on='countyName', suffixes=('_gov18', '_pres20'))

    margin_benchmark = df.margin_2party_gov18.apply(lambda x: x - statewide_margin).round(1)
    df['voteShareD_benchmark'] = margin_benchmark.apply(lambda x: 50 + (x / 2)).round(1)
    df['voteShareR_benchmark'] = margin_benchmark.apply(lambda x: 50 - (x / 2)).round(1)
    df['margin_benchmark'] = margin_benchmark.apply(lambda x: f'{"D" if x > 0 else "R"}+{abs(x)}')

    df.margin_2party_gov18 = df.margin_2party_gov18.apply(lambda x: f'{"Whitmer" if x > 0 else "Schuette"}+{abs(x)}')
    df.margin_2party_pres20 = df.margin_2party_pres20.apply(lambda x: f'{"Biden" if x > 0 else "Trump"}+{abs(x)}')

    df.to_csv('county_level_summaries/2022_statewide_benchmarks_for_tie.csv', index=False)


def create_district_level_summaries():
    def _create_one(args) -> pd.DataFrame:
        return (
            create_district_level_summary(*args)
                .drop(columns=['geometry']).rename(columns=dict(DISTRICTNO='district'))
                .assign(year=args[0]).assign(office=args[1])
        )

    office_name = 'congressional'
    filename_label = 'Congressional'

    options = [
        (2016, 'President of the United States', office_name),
        (2020, 'President of the United States', office_name),

        (2018, 'United States Senator', office_name),
        (2020, 'United States Senator', office_name),

        (2016, 'Representative in Congress', office_name),
        (2018, 'Representative in Congress', office_name),
        (2020, 'Representative in Congress', office_name),

        (2018, 'Governor', office_name),
    ]
    df = pd.concat(_create_one(i) for i in options)

    df.voteShareD_2party = df.voteShareD_2party.apply(lambda x: x * 100).round(1)
    df.voteShareR_2party = df.voteShareR_2party.apply(lambda x: x * 100).round(1)
    df.margin_2party = df.margin_2party.apply(lambda x: x * 100).round(1)

    df = df.sort_values(['district', 'year', 'office'])
    df = df[[
        'district', 'office', 'year', 'voteShareD_2party', 'voteShareR_2party', 'margin_2party', 'marginText_2party']]

    df.to_csv(f'2022_districts/{filename_label}.csv', index=False)
    return df
