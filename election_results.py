import geopandas as gpd
import pandas as pd

import shapes


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


def read_offices(year: int) -> pd.DataFrame:
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


def read_parties(year: int) -> pd.DataFrame:
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


def read_votes(year: int) -> pd.DataFrame:
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


def read_mcd(year: int) -> pd.DataFrame:
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


def read_counties(year: int) -> pd.DataFrame:
    dtype = {
        'county_code': int,
        'county_name': str,
    }
    counties = _read_file('county.txt', year, dtype)
    return counties


def read_mcd_fips_mapper() -> pd.DataFrame:
    mcd_fips_mapper = gpd.read_file('G:/election_data/MichiganShapefiles/MinorCivilDivisions.zip').iloc[:, 1:6].drop(
        columns='FIPSNUM')
    for col in ('LABEL', 'NAME', 'TYPE'):
        mcd_fips_mapper[col] = mcd_fips_mapper[col].apply(lambda x: x.upper())
    mcd_fips_mapper['NAME_TYPE'] = mcd_fips_mapper.NAME + ' ' + mcd_fips_mapper.TYPE
    return mcd_fips_mapper


def get_office_codes(offices: pd.DataFrame, office_name: str, one: bool = False) -> [list, pd.DataFrame]:
    df = offices.loc[offices.office_desc.str.contains(office_name.upper()), ['office_code', 'office_desc']]
    if one:
        return df.office_code.values[0]  # list
    else:
        return df  # dataframe


def merge_all(
        offices: pd.DataFrame,
        office_name: str,
        parties: pd.DataFrame,
        votes: pd.DataFrame,
        mcd: pd.DataFrame,
        counties: pd.DataFrame,
) -> pd.DataFrame:
    office_code = get_office_codes(offices, office_name, one=True)
    parties = parties[parties.office_code == office_code].drop(columns='office_code')
    votes = votes[votes.office_code == office_code].drop(columns='office_code')

    votes_merged = votes.merge(parties, on='candidate_id').merge(mcd, on=['mcd_code', 'county_code']).merge(
        counties, on='county_code').drop(columns=['county_code', 'mcd_code', 'candidate_id'])
    return votes_merged


def transpose_parties_into_columns(votes_rollup: pd.DataFrame) -> pd.DataFrame:
    merge_cols = ['county_name', 'mcd_name', 'FIPSCODE', 'WARD', 'PRECINCT']

    total_votes = votes_rollup.groupby(merge_cols, as_index=False).votes.sum().rename(columns=dict(votes='totalvot'))
    votes_grouped = votes_rollup.groupby(merge_cols + ['party'], as_index=False).votes.sum()

    def _process_party(p: str) -> pd.DataFrame:
        return votes_grouped[votes_grouped.party == p.upper()].drop(columns='party').rename(columns={
            'votes': f'{p[0].lower()}vot'})

    d = _process_party('dem')
    r = _process_party('rep')
    oth = _process_party('oth')

    votes_parties = d.merge(r, how='outer', on=merge_cols).merge(oth, how='outer', on=merge_cols)
    for party in ('d', 'r', 'o'):
        votes_parties[f'{party}vot'] = votes_parties[f'{party}vot'].fillna(0)
    votes_parties = votes_parties.merge(total_votes, on=merge_cols)
    return votes_parties


def get_election_results(year: int, office_name: str, county_name: str = None) -> pd.DataFrame:
    offices = read_offices(year)
    parties = read_parties(year)
    votes = read_votes(year)
    mcd = read_mcd(year)
    counties = read_counties(year)
    if county_name:
        counties = counties[counties.county_name == county_name.upper()].copy()

    election_results = merge_all(offices, office_name, parties, votes, mcd, counties)
    mcd_fips_mapper = read_mcd_fips_mapper()
    election_results.mcd_name = election_results.mcd_name.apply(_normalize_mcd_name)
    election_results = pd.concat(election_results.merge(mcd_fips_mapper, left_on='mcd_name', right_on=col) for col in (
        'LABEL', 'NAME', 'NAME_TYPE'))
    election_results = election_results.drop(columns=['LABEL', 'NAME', 'TYPE', 'NAME_TYPE'])
    # drop_duplicates suddenly doesn't accept the subset argument?
    # noinspection PyArgumentList
    election_results = election_results.drop_duplicates(subset=['FIPSCODE', 'WARD', 'PRECINCT', 'party', 'cand'])
    # which is then messing up the type of election_results
    # noinspection PyTypeChecker
    election_results = transpose_parties_into_columns(election_results)

    election_results = election_results.rename(columns={'FIPSCODE': 'MCDFIPS'})
    return election_results


def add_voteshare_and_margin(election_results: pd.DataFrame) -> pd.DataFrame:
    election_results['dvs'] = election_results.dvot.map(int) / election_results.totalvot.map(int)
    election_results['rvs'] = election_results.rvot.map(int) / election_results.totalvot.map(int)
    election_results['margin'] = election_results.dvs.map(float) - election_results.rvs.map(float)
    for col in ('dvs', 'rvs', 'margin'):
        election_results[col] = election_results[col].apply(lambda x: round(x, 3))
    election_results['winner'] = election_results.margin.apply(lambda x: 'd' if x > 0 else 'r')
    election_results['margin_text'] = election_results.margin.apply(
        lambda x: f'{"D" if x > 0 else "R"}+{int(round(abs(x) * 100))}')
    election_results = election_results.drop(columns=['dvot', 'rvot', 'ovot', 'totalvot'])
    return election_results


def create_summary(
        year: int,
        office_name: str,
        senate: bool = False,
        save_data: bool = False,
        save_plot: bool = False,
        filename_label: str = None,
) -> gpd.GeoDataFrame:
    df = get_election_results(year, office_name)
    df = df.merge(shapes.read_intersections(year, senate), on=['MCDFIPS', 'WARD', 'PRECINCT'])
    df = df.drop_duplicates(subset=['county_name', 'mcd_name', 'WARD', 'PRECINCT'])
    for col in ('dvot', 'rvot', 'ovot', 'totalvot'):
        df[col] = df[col] * df['intersection']
    df = df.groupby('DISTRICTNO', as_index=False).agg(dict(dvot=sum, rvot=sum, ovot=sum, totalvot=sum))
    df = df.merge(shapes.read_districts(senate), on='DISTRICTNO')
    df = add_voteshare_and_margin(df)
    df = gpd.GeoDataFrame(df.to_dict('records'))

    if save_data:
        df.drop(columns='geometry').to_csv(
            f'2022_districts/{filename_label} by {"S" if senate else "H"}D {year}.csv', index=False)

    if save_plot:
        plt = df.plot('margin', cmap='RdYlBu', legend='margin', vmin=-0.5, vmax=0.5)
        plt.set_title(f'{year} {filename_label} Results by State {"Senate" if senate else "House"} District')

    return df


def create_gubernatorial_comparison(hd_or_sd: str) -> pd.DataFrame:
    year1 = pd.read_csv(f'2022_districts/Gubernatorial by {hd_or_sd.upper()} 2014.csv', usecols=[
        'DISTRICTNO', 'margin', 'winner'])
    year2 = pd.read_csv(f'2022_districts/Gubernatorial by {hd_or_sd.upper()} 2018.csv', usecols=[
        'DISTRICTNO', 'margin', 'winner'])
    df = year1.merge(year2, on='DISTRICTNO', suffixes=('2014', '2018'))
    df['margin_avg'] = (df.margin2014 + df.margin2018).apply(lambda x: round(x / 2, 2))
    df = df.rename(columns=dict(DISTRICTNO='district'))
    return df


def main():
    for year in (2014, 2018):
        for d in ('HD', 'SD'):
            df = create_summary(year, dict(HD='REPRESENTATIVE IN STATE LEG', SD='STATE SENATOR')[d], d == 'SD').drop(
                columns=['geometry'])
            df.to_csv(f'2022_districts/Gubernatorial by {d} {year}.csv')


if __name__ == '__main__':
    main()
