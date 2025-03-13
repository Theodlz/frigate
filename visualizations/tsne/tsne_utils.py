import pandas as pd
import numpy as np
import pickle
from astropy.coordinates import SkyCoord
import astropy.units as u
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from datetime import datetime


class alert_preprocessor:
    def __init__(
        self,
        path,
        drb_cut=0.4,
        filtered_only=False,
        remove_instrumental=True,
        custom_columns=[],
        edit_filters=False,
    ):
        self.path = path
        self.drb_cut = drb_cut
        self.filtered_only = filtered_only
        self.custom_columns = custom_columns
        self.remove_instrumental = remove_instrumental
        self.edit_filters = edit_filters

    def load_data(self):
        """
        load the data as a pandas dataframe
        """
        df = pd.read_parquet(self.path)
        return df

    def remove_filters(self, arr):
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

    def parameter_modifications(self, df):
        # add age parameter
        df["age"] = df["candidate.jd"] - df["candidate.jdstarthist"]
        df["lastobs"] = df["candidate.jd"] - df["candidate.jdendhist"]
        # use ra and dec to get galactic latitude
        ra = df["candidate.ra"]
        dec = df["candidate.dec"]
        coords = SkyCoord(
            ra=ra.values * u.degree, dec=dec.values * u.degree, frame="icrs"
        )
        galactic_coords = coords.galactic
        galactic_latitudes = galactic_coords.b.deg
        df["galactic_latitude"] = galactic_latitudes
        # parameter reformatting
        df["candidate.isdiffpos"] = df["candidate.isdiffpos"].map({"f": 0, "t": 1})
        if self.edit_filters:
            df["filtered_bool"] = df["passed_filters"].apply(
                lambda x: 0 if len(x) == 0 else 1
            )
        return df

    def shift_errors(self, df):
        """
        reassign -999 error values to be just outside the range of the data
        """
        for column in df.columns:
            if pd.api.types.is_numeric_dtype(df[column]):
                valid_values = df[column][df[column] > -900]
                if not valid_values.empty:
                    min_value = valid_values.min()
                    value_range = valid_values.max() - min_value
                    new_value = min_value - 0.1 * value_range
                    df[column] = df[column].apply(
                        lambda x: new_value if x < -900 else x
                    )
        return df

    def edit_columns(self, df, custom_columns=[], remove_instrumental=True):
        """
        remove some columns from consideration, rename columns
        """
        if len(custom_columns) > 0:
            print(f"these are the custom columns: {custom_columns}")
            df = df[custom_columns]
        else:
            ignore = [
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

        if remove_instrumental:
            instrumental = [
                "candidate.mindtoedge",
                "candidate.nneg",
                "candidate.nbad",
                "candidate.dsnrms",
                "candidate.ssnrms",
                "candidate.dsdiff",
                "candidate.nmatches",
                "candidate.clrcoeff",
                "candidate.clrcounc",
                "candidate.zpclrcov",
                "candidate.zpmed",
                "candidate.clrmed",
                "candidate.clrrms",
                "candidate.exptime",
            ]
            df = df.drop(columns=instrumental, errors="ignore")

        df.columns = df.columns.str.replace(r"^candidate\.", "", regex=True)
        df.columns = df.columns.str.replace(r"^classifications\.", "", regex=True)
        return df

    def preprocess_data(self):
        df = self.load_data()
        if self.edit_filters:
            if self.filtered_only:
                df = df[df["filtered_bool"] == 1]
            df["passed_filters"] = df["passed_filters"].apply(self.remove_filters)
        df = df[df["candidate.drb"] > self.drb_cut]  # cut likely bogus alerts
        df = self.parameter_modifications(df)
        df = self.shift_errors(df)
        df = self.edit_columns(
            df,
            custom_columns=self.custom_columns,
            remove_instrumental=self.remove_instrumental,
        )
        return df


class prep_TSNE:
    def __init__(self, df, use_PCA=True, pca_ncomp=40):
        self.df = df
        self.use_PCA = use_PCA
        self.pca_ncomp = pca_ncomp

    def drop_for_training(self):
        """
        drop labels for training
        """
        self.df = self.df.drop(
            columns=[
                "objectId",
                "candid",
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
        return self.df

    def normalize_data(self):
        """
        normalize the data with z-score transformation
        """
        scaler = StandardScaler()
        data_scaled = scaler.fit_transform(self.df)
        self.df = pd.DataFrame(data_scaled, columns=self.df.columns)
        return self.df

    def get_pca(self):
        """
        perform PCA transformation
        """
        pca = PCA(n_components=self.pca_ncomp)
        pca_result = pca.fit_transform(self.df)
        print(
            f"Cumulative explained variation for {self.pca_ncomp} principal components: {np.sum(pca.explained_variance_ratio_)}"
        )
        return pca_result

    def prep_data(self):
        self.df = self.drop_for_training()
        self.df = self.normalize_data()
        if self.use_PCA:
            data = self.get_pca()
        else:
            data = self.df
        return data


class tSNE:
    def __init__(
        self,
        pca_result,
        perplexity=60,
        early_exaggeration=12,
        learning_rate="auto",
        max_iter=2000,
        method="barnes_hut",
        n_jobs=8,
        save_path="default",
    ):
        self.pca_result = pca_result
        self.perplexity = perplexity
        self.early_exaggeration = early_exaggeration
        self.learning_rate = learning_rate
        self.max_iter = max_iter
        self.method = method
        self.n_jobs = n_jobs
        self.save_path = save_path

    def get_tsne(self):
        tsne = TSNE(
            n_components=2,
            verbose=0,
            perplexity=self.perplexity,
            early_exaggeration=self.early_exaggeration,
            learning_rate=self.learning_rate,
            max_iter=self.max_iter,
            method=self.method,
            n_jobs=self.n_jobs,
        )
        tsne_results = tsne.fit_transform(self.pca_result)
        if self.save_path:
            if self.save_path == "default":
                filename = f"tsne_{datetime.now()}.pkl"
                tsne_save_path = f"../private_data/tsne_trained/{filename}"
            else:
                tsne_save_path = self.save_path
                filename = tsne_save_path.split("/")[-1]
            with open(tsne_save_path, "wb") as f:
                pickle.dump(tsne_results, f)
        return tsne_results, filename
