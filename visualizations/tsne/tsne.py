import pandas as pd
import numpy as np
import pickle
from astropy.coordinates import SkyCoord
import astropy.units as u
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from datetime import datetime


def load_data(path):
    """
    load the data as a pandas dataframe
    """
    df = pd.read_parquet(path)
    return df


def remove_filters(arr):
    """
    remove filters that aren't in use or test filters
    """
    if arr.size == 0:
        return arr
    remove_values = {
        20,
        55,
        63,
        64,
        65,
        66,
        67,
        68,
        69,
        70,
        71,
        74,
        75,
        76,
        79,
        81,
        89,
        90,
        100,
        102,
        103,
        106,
        1159,
        1162,
        1163,
        1164,
        1168,
        1181,
    }
    mask = np.vectorize(lambda x: x not in remove_values)(arr)
    return arr[mask]


def parameter_modifications(df):
    # add age parameter
    df["age"] = df["candidate.jd"] - df["candidate.jdstarthist"]
    df["lastobs"] = df["candidate.jd"] - df["candidate.jdendhist"]
    # use ra and dec to get galactic latitude
    ra = df["candidate.ra"]
    dec = df["candidate.dec"]
    coords = SkyCoord(ra=ra.values * u.degree, dec=dec.values * u.degree, frame="icrs")
    galactic_coords = coords.galactic
    galactic_latitudes = galactic_coords.b.deg
    df["galactic_latitude"] = galactic_latitudes
    # parameter reformatting
    df["candidate.isdiffpos"] = df["candidate.isdiffpos"].map({"f": 0, "t": 1})
    df["filtered_bool"] = df["passed_filters"].apply(lambda x: 0 if len(x) == 0 else 1)
    return df


def edit_columns(df, remove_instrumental=True):
    """
    remove some columns from consideration, rename columns
    """
    ignore = [
        "candid",
        "candidate.jd",
        "candidate.pid",
        "candidate.programid",
        "candidate.tblid",
        "candidate.nid",
        "candidate.rcid",
        "candidate.field",
        "candidate.xpos",
        "candidate.ypos",
        "candidate.rbversion",
        "candidate.drbversion",
        "candidate.ssnamenr",
        "candidate.ranr",
        "candidate.decnr",
        "candidate.tooflag",
        "candidate.objectidps1",
        "candidate.objectidps2",
        "candidate.objectidps3",
        "candidate.rfid",
        "candidate.jdstartref",
        "candidate.jdendref",
        "candidate.nframesref",
        "classifications.braai_version",
        "classifications.acai_h_version",
        "classifications.acai_v_version",
        "classifications.acai_o_version",
        "classifications.acai_n_version",
        "classifications.acai_b_version",
        "classifications.bts_version",
        "candidate.jdstarthist",
        "candidate.jdendhist",
        "classifications.acai_h",
        "classifications.acai_v",
        "classifications.acai_o",
        "classifications.acai_n",
        "classifications.acai_b",
    ]
    df = df.drop(columns=[col for col in ignore if col in df.columns])
    instrumental = [
        "mindtoedge",
        "nneg",
        "nbad",
        "dsnrms",
        "ssnrms",
        "dsdiff",
        "nmatches",
        "clrcoeff",
        "clrcounc",
        "zpclrcov",
        "zpmed",
        "clrmed",
        "clrrms",
        "exptime",
    ]
    if remove_instrumental:
        df = df.drop(columns=instrumental, errors="ignore")
    df.columns = df.columns.str.replace(r"^candidate\.", "", regex=True)
    df.columns = df.columns.str.replace(r"^classifications\.", "", regex=True)
    return df


class tSNEDataPreprocessor:
    def __init__(self, path, drb_cut=0.4):
        self.path = path
        self.drb_cut = drb_cut

    def preprocess_data(self):
        df = load_data(self.path)
        df["passed_filters"] = df["passed_filters"].apply(remove_filters)
        df = df[df["candidate.drb"] > self.drb_cut]  # cut likely bogus alerts
        df = parameter_modifications(df)
        df = edit_columns(df)
        return df

    def drop_for_training(self, df):
        """
        drop labels for training
        """
        df = df.drop(
            columns=[
                "objectId",
                "fid",
                "passed_filters",
                "fritz_classification",
                "simbad_classification",
                "scope_classification",
                "sdss_match",
                "filtered_bool",
                "number_filtered",
                "class",
                "acai",
                "catnorth",
                "gaia",
                "ra",
                "dec",
            ],
            errors="ignore",
        )
        return df

    def normalize_data(self, df):
        """
        normalize the data with z-score transformation
        """
        scaler = StandardScaler()
        data_scaled = scaler.fit_transform(df)
        df_normalized = pd.DataFrame(data_scaled, columns=df.columns)
        return df_normalized

    def get_pca(self, df, ncomp=40):
        """
        perform PCA transformation
        """
        df = self.drop_for_training(df)
        df = self.normalize_data(df)
        pca = PCA(n_components=ncomp)
        pca_result = pca.fit_transform(df)
        print(
            f"Cumulative explained variation for 50 principal components: {np.sum(pca.explained_variance_ratio_)}"
        )
        return pca_result


class tSNE:
    def __init__(
        self,
        pca_result,
        perplexity=60,
        max_iter=2000,
        method="barnes_hut",
        n_jobs=8,
        save_path=None,
        show_progress=True,
    ):
        self.pca_result = pca_result
        self.perplexity = perplexity
        self.max_iter = max_iter
        self.method = method
        self.n_jobs = n_jobs
        self.save_path = save_path

    def get_tsne(self):
        tsne = TSNE(
            n_components=2,
            verbose=0,
            perplexity=self.perplexity,
            max_iter=self.max_iter,
            method=self.method,
            n_jobs=self.n_jobs,
        )
        tsne_results = tsne.fit_transform(self.pca_result)
        if self.save_path:
            if self.save_path == "default":
                time = datetime.now()
                tsne_save_path = f"../example_data/tsne_results_{time}.pkl"
            else:
                tsne_save_path = self.save_path
            with open(tsne_save_path, "wb") as f:
                pickle.dump(tsne_results, f)
        return tsne_results
