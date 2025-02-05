import pandas as pd
import yaml
import requests
import pickle
from penquins import Kowalski
from astropy.time import Time
from astroquery.simbad import Simbad
from astropy.coordinates import SkyCoord
import astropy.units as u


class MyException(Exception):
    pass


# boookkeeping

with open("../../credentials.yaml") as file:
    credentials = yaml.safe_load(file)
fritz_token = credentials["fritz_token"]
kowalski_password = credentials["kowalski_password"]


def get_filtered_subset(df):
    """
    save processing time look only at filtered alerts
    """
    filtered_df = df[df["passed_filters"].str.len() > 0]
    return filtered_df


def add_class_to_df(df, column_name, object_id, class_value):
    """
    add classification to the dataframe
    """
    df[column_name] = None
    df.loc[df["objectId"] == object_id, column_name] = class_value


# fritz groups assigned


class FritzNightClassifications:
    def __init__(self, token):
        self.token = token

    def get_night_classifications(self, df, add_to_df=True):
        # get dates
        min_jd, max_jd = df["candidate.jd"].min(), df["candidate.jd"].max()
        min_time, max_time = Time(min_jd, format="jd"), Time(max_jd, format="jd")
        startdate, enddate = min_time.utc.strftime("%Y-%m-%d"), max_time.utc.strftime(
            "%Y-%m-%d"
        )

        # api call
        endpoint = f"https://fritz.science/api/classification?startDate={startdate}&endDate={enddate}&numPerPage=500"
        headers = {"Authorization": f"token {self.token}"}
        response = requests.request("GET", endpoint, headers=headers)

        if response.status_code != 200:
            raise MyException(
                f"Could not retrieve classification - {response.status_code} - {response.text}"
            )

        data = response.json()
        obj_id = [
            classification["obj_id"]
            for classification in data["data"]["classifications"]
        ]
        value = [
            classification["classification"]
            for classification in data["data"]["classifications"]
        ]

        if add_to_df:
            add_class_to_df(df, "fritz_night_classification", obj_id, value)

        return data, df


# SIMBAD classifications


class SimbadClassifications:
    def __init__(self):
        pass

    @staticmethod
    def query_simbad(coord):
        """
        Function to query SIMBAD for a single coordinate
        """
        custom_simbad = Simbad()
        custom_simbad.add_votable_fields(
            "ra", "dec", "otype", "otype(V)", "sptype", "distance"
        )
        result = custom_simbad.query_region(coord, radius="0d0m3s")
        return result

    def get_simbad_classifications(self, df, save_query_path, add_to_df=True):
        df = get_filtered_subset(df)  # this query is slow
        coords = [(x, y) for x, y in zip(df["candidate.ra"], df["candidate.dec"])]
        sky_coords = [
            SkyCoord(ra=ra * u.degree, dec=dec * u.degree, frame="icrs")
            for ra, dec in coords
        ]
        simbad_results = []
        for coord in sky_coords:
            try:
                result = self.query_simbad(coord)
                value = str(list(result["OTYPE_V"].data))
            except ConnectionError as e:
                print(f"Connection error querying SIMBAD for {coord}: {e}")
                value = None
            except Exception as e:
                print(f"Error querying SIMBAD for {coord}: {e}")
                value = None
            simbad_results.append(value)
        obj_ids = df["objectId"].values
        if save_query_path:
            simbad_dict = {
                object_id: result for object_id, result in zip(obj_ids, simbad_results)
            }
            with open(save_query_path, "wb") as f:
                pickle.dump(simbad_dict, f)

        if add_to_df:
            add_class_to_df(df, "simbad_classification", obj_ids, simbad_results)

        return simbad_results, df


# Kowalski for catalog classifications


class CatalogClassifications:
    def __init__(self, kowalski_password):
        self.kowalski_password = kowalski_password
        self.kowalski = None

    def connect_to_kowalski(self):
        instances = {
            "kowalski": {
                "name": "kowalski",
                "host": "kowalski.caltech.edu",
                "protocol": "https",
                "port": 443,
                "username": "knolan",
                "password": self.kowalski_password,
                "timeout": 6000,
            },
            "gloria": {
                "name": "gloria",
                "host": "gloria.caltech.edu",
                "protocol": "https",
                "port": 443,
                "username": "knolan",
                "password": self.kowalski_password,
                "timeout": 6000,
            },
        }
        self.kowalski = Kowalski(instances=instances)

    def kowalski_catalog_conesearch(
        self, df, catalog, projection, machine, add_to_df=True
    ):
        ra_list = df["candidate.ra"].values
        dec_list = df["candidate.dec"].values
        obj_ids = df["objectId"].values

        queries = [
            {
                "query_type": "cone_search",
                "query": {
                    "object_coordinates": {
                        "cone_search_radius": 3,
                        "cone_search_unit": "arcsec",
                        "radec": {object_name: [ra, dec]},
                    },
                    "catalogs": {catalog: {"filter": {}, "projection": projection}},
                },
                "kwargs": {"filter_first": False},
            }
            for object_name, ra, dec in zip(obj_ids, ra_list, dec_list)
        ]
        responses = self.kowalski.query(
            queries=queries, name=machine, use_batch_query=True, max_n_threads=4
        )
        data = [x.get("data", []).get(catalog, []) for x in responses.get(machine, {})]
        match = [True if v else False for d in data for k, v in d.items()]
        if add_to_df:
            add_class_to_df(df, f"{catalog}_classification", obj_ids, match)
        return data, df


# check against catalog of Fritz classifications


class FritzClassifications:
    def __init__(self):
        pass

    def get_fritz_classes(self, df, add_to_df=True):
        fritz_classification_catalog = pd.read_csv("../example_data/frigateclasses.csv")
        obj_ids = df["objectId"].values
        fritz_dict = dict(
            zip(
                fritz_classification_catalog["obj_id"],
                fritz_classification_catalog["type"],
            )
        )
        match = [fritz_dict.get(obj_id, None) for obj_id in obj_ids]
        if add_to_df:
            add_class_to_df(df, "fritz_catalog_classification", obj_ids, match)
        return match, df


# assign classification based on acai scores


class AcaiClassifications:
    def __init__(self):
        pass

    def get_acai_classes(self, df, add_to_df=True):
        def determine_acai(row):
            conditions = {
                "H": row["classifications.acai_h"] > 0.8,
                "V": row["classifications.acai_v"] > 0.8,
                "O": row["classifications.acai_o"] > 0.8,
                "N": row["classifications.acai_n"] > 0.8,
                "B": row["classifications.acai_b"] > 0.8,
            }
            true_conditions = [key for key, value in conditions.items() if value]
            if len(true_conditions) == 1:
                return true_conditions[0]
            else:
                return "ambiguous"

        acai = df.apply(determine_acai, axis=1).tolist()

        if add_to_df:
            add_class_to_df(df, "acai_classification", df["objectId"].values, acai)

        return acai, df


# assign classification based on what filters passed alerts and what they should look for


class FilterClassifications:
    def __init__(self):
        pass

    def get_filter_classes(self, df, add_to_df=True):
        # Create a dictionary of the classes of objects different filters should look for
        class_dict = {
            "SNe": [1, 3, 9, 11, 13, 1174, 1176, 1178, 1179, 1180, 1182, 107],
            "GRB": [8, 1160],
            "CV": [105],
            "YSO": [111, 112],
            "TDE": [121],
            "None": [],
        }

        def determine_class(row):
            passed_filters = row["filters"]
            classes = []
            for class_name, values in class_dict.items():
                if any(item in passed_filters for item in values):
                    classes.append(class_name)
            return ", ".join(classes) if classes else None

        filter_class = df.apply(determine_class, axis=1).tolist()

        if add_to_df:
            add_class_to_df(
                df, "filter_classification", df["objectId"].values, filter_class
            )

        return filter_class, df
