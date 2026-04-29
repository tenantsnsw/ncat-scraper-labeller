import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

import pandas as pd
import constants
import numpy as np
import datetime as dt
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import classification_report
from tqdm import tqdm
from core.logger_config import get_logger, post_log_update, format_traceback
from core.profiler import profile_resources
import pickle
import random
import time
import label_loading_saving as ls
from functools import reduce
from dateutil.relativedelta import relativedelta
import sys

log = get_logger(__file__)
verbose = True  # module-level default; __main__ may override


# from tqdm.auto import tqdm  # for notebooks
tqdm.pandas()
# Gets rid of warning:
# |A value is trying to be set on a copy of a slice from a DataFrame.
# |Try using .loc[row_indexer,col_indexer] = value instead
pd.options.mode.copy_on_write = True


def build_bag_of_words_features(features_df, max_features=5000, verbose=True):
    if verbose:
        print("Building Bag of Words Features...")
    vectorizer = CountVectorizer(
        analyzer="word",
        tokenizer=None,
        preprocessor=None,
        stop_words=None,
        max_features=max_features,
    )

    sparse_matrix = vectorizer.fit_transform(features_df["Normalised Applicant"])
    arr = sparse_matrix.astype(np.int16).toarray()
    del sparse_matrix
    train_data_features = pd.DataFrame(
        arr, columns=vectorizer.get_feature_names_out()
    )
    del arr
    print("Size", train_data_features.shape)
    train_data_features["Normalised Applicant"] = features_df[
        "Normalised Applicant"
    ].to_list()
    bag_of_words_cols = list(train_data_features.columns)
    bag_of_words_cols.remove("Normalised Applicant")
    return train_data_features, bag_of_words_cols


def build_list_total_features(data, verbose=True):
    if verbose:
        print("Building List Total Features...")
    list_lookup = data[["Normalised Applicant", "List"]].value_counts().reset_index()
    list_lookup = list_lookup.set_index(["Normalised Applicant", "List"])
    list_scores = data[["Normalised Applicant"]].drop_duplicates()

    def list_score_dict(applicant):
        return list_lookup.loc[applicant].to_dict()["count"]

    list_scores["List Score"] = list_scores["Normalised Applicant"].progress_apply(
        list_score_dict
    )
    listing_type_totals = (
        pd.json_normalize(list_scores["List Score"])
        .fillna(0)
        .rename(
            columns={
                "": "Unlabeled Listings",
                "SH": "Social Housing Listings",
                "RT": "Residential Tenancy Listings",
            }
        )
    )
    other_listings_types = [
        "HB",
        "AP",
        "COM",
        "SC",
        "GEN",
        "SCS",
        "MV",
        "RC",
        "RV",
        "RP",
        "CL",
        "PC",
    ]
    listing_type_totals["Other Listings"] = listing_type_totals[
        other_listings_types
    ].sum(axis=1)
    listing_total_cols = [
        "Unlabeled Listings",
        "Social Housing Listings",
        "Residential Tenancy Listings",
        "Other Listings",
    ]
    listing_type_totals = listing_type_totals[listing_total_cols]
    listing_type_totals["Total Listings"] = listing_type_totals.sum(axis=1)

    for col in listing_total_cols:
        listing_type_totals[col] = (
            listing_type_totals[col] / listing_type_totals["Total Listings"]
        )

    listing_type_totals = listing_type_totals[listing_total_cols]
    listing_type_totals["Normalised Applicant"] = list_scores[
        "Normalised Applicant"
    ].to_list()
    return listing_type_totals


def feature_cols(features_df, bag_of_words_cols):
    sum_jaccard_score_cols = [
        col
        for col in features_df.select_dtypes(["float64", "int64"]).columns
        if ("Sum Score") in col
    ]
    listing_total_cols = [
        "Unlabeled Listings",
        "Social Housing Listings",
        "Residential Tenancy Listings",
        "Other Listings",
    ]
    return sum_jaccard_score_cols + bag_of_words_cols + listing_total_cols


def hypertune_model(X_train, y_train):
    param_grid = {
        "n_estimators": np.linspace(10, 400, dtype=np.dtype(np.int16)),
        "max_features": ["log2", "sqrt"],
        "max_depth": np.linspace(100, 600, dtype=np.dtype(np.int16)),
        "bootstrap": [True, False],
        "criterion": ["entropy"],
    }
    clf = make_pipeline(
        StandardScaler(),
        RandomizedSearchCV(
            RandomForestClassifier(verbose=1),
            param_distributions=param_grid,
            cv=5,
            n_iter=50,
            verbose=1,
            n_jobs=-1,
            scoring=[
                "recall_weighted",
                "precision_weighted",
                "f1_weighted",
                "accuracy",
            ],
            refit="recall_weighted",
        ),
    )
    clf.fit(X_train, y_train)
    return clf


def produce_classification_report(
    clf, code_lookup_df, X_test, y_test, true_col, return_key_metrics=False
):
    cr = classification_report(
        code_lookup_df.loc[y_test, true_col],
        code_lookup_df.loc[clf.predict(X_test), true_col],
        output_dict=True,
        zero_division=0,
    )
    crdf = pd.DataFrame(cr).transpose()
    crdf = crdf.drop(
        [
            "NOT A SOCIAL HOUSING PROVIDER",
            "Other",
            "accuracy",
            "macro avg",
            "weighted avg",
        ]
    )
    unique_providers = len(crdf) + 1
    cols = ["precision", "recall", "f1-score"]
    macro_avg_row = dict(crdf[cols].mean(axis=0)) | dict(crdf[["support"]].sum(axis=0))
    for col in cols:
        crdf[f"weighted {col}"] = crdf[col] * crdf["support"]
    weighted_avg_row = dict(
        crdf[[f"weighted {col}" for col in cols]].sum(axis=0) / macro_avg_row["support"]
    )
    weighted_avg_row["support"] = macro_avg_row["support"]
    weighted_avg_row = {
        key.replace("weighted ", ""): value for key, value in weighted_avg_row.items()
    }
    crdf = crdf.drop(columns=[f"weighted {col}" for col in cols])
    crdf.loc["macro avg"] = macro_avg_row
    crdf.loc["weighted avg"] = weighted_avg_row
    if return_key_metrics:
        return crdf, {"unique_providers": unique_providers} | dict(
            crdf.loc["weighted avg"]
        )
    else:
        return crdf


def pkl_model(model):
    today_str = pd.Timestamp.today().strftime("%Y%m%d")
    now_str = pd.Timestamp.today().strftime("%H%M%S")
    model_pkl = constants.PKL_DIR / f"model_{today_str}_{now_str}.pkl"
    with open(model_pkl, "wb") as f:
        pickle.dump(model, f)


def pkl_params(params):
    today_str = pd.Timestamp.today().strftime("%Y%m%d")
    now_str = pd.Timestamp.today().strftime("%H%M%S")
    params_pkl = constants.PKL_DIR / f"params_{today_str}_{now_str}.pkl"
    with open(params_pkl, "wb") as f:
        pickle.dump(params, f)  # serialize the list


def load_latest_model():
    def model_filename_to_datetime(filename):
        filename = filename.split("model_")[-1]
        datetime = filename.split(".pkl")[0]
        return datetime

    def latest_model_pkl():
        all_model_pkls = [f for f in constants.PKL_DIR.iterdir() if "model_" in f.name]
        all_datetimes = [model_filename_to_datetime(f.name) for f in all_model_pkls]
        all_datetimes = [
            dt.datetime.strptime(d, "%Y%m%d_%H%M%S") for d in all_datetimes
        ]
        latest_model = np.array(all_datetimes).max()
        latest_model = (
            constants.PKL_DIR / f"model_{latest_model.strftime('%Y%m%d_%H%M%S')}.pkl"
        )
        return latest_model

    model_pkl = latest_model_pkl()
    with open(model_pkl, "rb") as f:
        selected_model = pickle.load(f)
    return selected_model


def load_latest_model_params():
    def params_filename_to_datetime(filename):
        filename = filename.split("params_")[-1]
        datetime = filename.split(".pkl")[0]
        return datetime

    def latest_params_pkl():
        all_params_pkls = [
            f for f in constants.PKL_DIR.iterdir() if "params_" in f.name
        ]
        if not all_params_pkls:
            return None
        all_datetimes = [params_filename_to_datetime(f.name) for f in all_params_pkls]
        all_datetimes = [
            dt.datetime.strptime(d, "%Y%m%d_%H%M%S") for d in all_datetimes
        ]
        latest_params = np.array(all_datetimes).max()
        latest_params = (
            constants.PKL_DIR / f"params_{latest_params.strftime('%Y%m%d_%H%M%S')}.pkl"
        )
        return latest_params

    params_pkl = latest_params_pkl()
    if params_pkl is None:
        return None
    with open(params_pkl, "rb") as f:
        selected_params = pickle.load(f)
    return selected_params


def make_predictions(data, selected_model, splits=4, n_attempts=0, max_attempts=3):
    try:
        y_pred = np.array([])
        print(f"Attempting make_predictions, attempt: {n_attempts}")
        for data_part in tqdm(np.array_split(data, splits)):
            y_pred_part = selected_model.predict(data_part)
            y_pred = np.concatenate((y_pred, y_pred_part))
        return y_pred
    except Exception as e:
        print(e)
        splits = 2 * splits
        n_attempts += 1
        print(f"Attempt make_predictions() failed, n_attempts {n_attempts}")
        if n_attempts >= max_attempts:
            print(f"Reached max attempts at make_predictions() {max_attempts}")
            raise e
        print(f"Trying again with more splits: {splits}")
        return make_predictions(
            data,
            selected_model,
            splits=splits,
            n_attempts=n_attempts,
            max_attempts=max_attempts,
        )


def build_data(max_labels=50, verbose=True):

    applicant_labels = ls.load_applicant_labels(set_vars=True)
    data = ls.load_listings(update_labels=True, applicant_labels=applicant_labels)
    features_df = ls.load_features(
        update_labels=True, applicant_labels=applicant_labels
    )

    train_data_features, bag_of_words_cols = build_bag_of_words_features(
        features_df, max_features=5000, verbose=verbose
    )
    features_df = features_df.merge(train_data_features, how="left")
    del train_data_features

    features_df = features_df.merge(
        build_list_total_features(data, verbose=verbose), how="left"
    )

    selected_cols = feature_cols(features_df, bag_of_words_cols)
    features_df, model_df, code_lookup_df = ls.reduce_applicant_labels(
        features_df, applicant_labels, data, max_labels=max_labels
    )

    return data, applicant_labels, features_df, model_df, code_lookup_df, selected_cols


def build_to_label(
    features_df,
    y_pred,
    code_lookup_df,
    pred_col,
    true_col,
    mode="week",
    random_label=True,
    latest_data=True,
    data=None,
):
    if isinstance(mode, str):
        mode = [mode]
    to_label_cols = [
        "Normalised Applicant",
        "Applicant Label",
        "Entity Classification",
        "Social Housing Provider",
    ]
    to_label = features_df[to_label_cols]
    to_label.loc[:, pred_col] = list(code_lookup_df.loc[y_pred, true_col])

    if data is None:
        data = ls.load_listings(update_labels=True, applicant_labels=applicant_labels)
    if pred_col in data.columns:
        data = data.drop(columns=pred_col)
    data = data.merge(to_label[["Normalised Applicant", pred_col]], how="left")

    sample_to_label = []
    to_label_supplementary_data = []
    if "all" in mode:
        mode_listings = data
        to_label_supplementary_data_df = build_to_label_supplementary_data(
            mode_listings,
            columns=[
                "Cases",
                "Latest Date",
                "Latest Case Number",
                "Latest Case Name",
            ],
        )
        to_label_supplementary_data_df["mode"] = "all"
        to_label_supplementary_data.append(to_label_supplementary_data_df)
        sample_to_label.extend(
            to_label[
                to_label["Normalised Applicant"].isin(
                    mode_listings["Normalised Applicant"].to_list()
                )
            ].index
        )
    if "random" in mode:

        mode_listings = data
        mode_listings_unlabelled = mode_listings[mode_listings["Applicant Label"] == ""]
        mode_listings_labelled = mode_listings[mode_listings["Applicant Label"] != ""]
        random_sample_to_label = []

        for label in code_lookup_df["Reduced Applicant Label"]:
            # Sample up to 5 labelled data points
            to_sample_list = list(
                mode_listings_labelled.loc[
                    mode_listings_labelled[pred_col] == label, "Normalised Applicant"
                ]
            )
            random_sample_to_label.extend(
                random.sample(to_sample_list, k=min(5, len(to_sample_list)))
            )
            # Sample up to 15 unlabelled data points
            to_sample_list = list(
                mode_listings_unlabelled.loc[
                    mode_listings_unlabelled[pred_col] == label, "Normalised Applicant"
                ]
            )
            random_sample_to_label.extend(
                random.sample(to_sample_list, k=min(15, len(to_sample_list)))
            )
        mode_listings = mode_listings.loc[
            mode_listings["Normalised Applicant"].isin(random_sample_to_label)
        ]
        to_label_supplementary_data_df = build_to_label_supplementary_data(
            mode_listings
        )
        to_label_supplementary_data_df["mode"] = "random"
        to_label_supplementary_data.append(to_label_supplementary_data_df)
        sample_to_label.extend(
            to_label[
                to_label["Normalised Applicant"].isin(
                    mode_listings["Normalised Applicant"].to_list()
                )
            ].index
        )
    if "today" in mode:

        mode_listings = data.loc[data["Date"] == pd.to_datetime("now").normalize()]
        to_label_supplementary_data_df = build_to_label_supplementary_data(
            mode_listings
        )
        to_label_supplementary_data_df["mode"] = "today"
        to_label_supplementary_data.append(to_label_supplementary_data_df)
        sample_to_label.extend(
            to_label[
                to_label["Normalised Applicant"].isin(
                    mode_listings["Normalised Applicant"].to_list()
                )
            ].index
        )
    if "month" in mode:

        mode_listings = data.loc[
            data["Date"].dt.strftime("%Y-%m") == pd.to_datetime("now").strftime("%Y-%m")
        ]
        to_label_supplementary_data_df = build_to_label_supplementary_data(
            mode_listings
        )
        to_label_supplementary_data_df["mode"] = "month"
        to_label_supplementary_data.append(to_label_supplementary_data_df)
        sample_to_label.extend(
            to_label[
                to_label["Normalised Applicant"].isin(
                    mode_listings["Normalised Applicant"].to_list()
                )
            ].index
        )
    if "last_month" in mode:
        mode_string = "last_month"
        mode_listings = data.loc[
            data["Date"].dt.strftime("%Y-%m")
            == (pd.to_datetime("now") + relativedelta(months=-1)).strftime("%Y-%m")
        ]
        to_label_supplementary_data_df = build_to_label_supplementary_data(
            mode_listings
        )
        to_label_supplementary_data_df["mode"] = mode_string
        to_label_supplementary_data.append(to_label_supplementary_data_df)
        sample_to_label.extend(
            to_label[
                to_label["Normalised Applicant"].isin(
                    mode_listings["Normalised Applicant"].to_list()
                )
            ].index
        )
    if "next_month" in mode:
        mode_string = "next_month"
        mode_listings = data.loc[
            data["Date"].dt.strftime("%Y-%m")
            == (pd.to_datetime("now") + relativedelta(months=+1)).strftime("%Y-%m")
        ]
        to_label_supplementary_data_df = build_to_label_supplementary_data(
            mode_listings
        )
        to_label_supplementary_data_df["mode"] = mode_string
        to_label_supplementary_data.append(to_label_supplementary_data_df)
        sample_to_label.extend(
            to_label[
                to_label["Normalised Applicant"].isin(
                    mode_listings["Normalised Applicant"].to_list()
                )
            ].index
        )
    if "week" in mode:

        mode_listings = data.loc[
            data["Date"].dt.strftime("%Y-%W") == pd.to_datetime("now").strftime("%Y-%W")
        ]
        to_label_supplementary_data_df = build_to_label_supplementary_data(
            mode_listings
        )
        to_label_supplementary_data_df["mode"] = "week"
        to_label_supplementary_data.append(to_label_supplementary_data_df)
        sample_to_label.extend(
            to_label[
                to_label["Normalised Applicant"].isin(
                    mode_listings["Normalised Applicant"].to_list()
                )
            ].index
        )
    if "last_week" in mode:
        mode_listings = data.loc[
            data["Date"].dt.strftime("%Y-%W")
            == (pd.to_datetime("now") - dt.timedelta(days=7)).strftime("%Y-%W")
        ]
        to_label_supplementary_data_df = build_to_label_supplementary_data(
            mode_listings
        )
        to_label_supplementary_data_df["mode"] = "last_week"
        to_label_supplementary_data.append(to_label_supplementary_data_df)
        sample_to_label.extend(
            to_label[
                to_label["Normalised Applicant"].isin(
                    mode_listings["Normalised Applicant"].to_list()
                )
            ].index
        )
    if "next_week" in mode:
        mode_listings = data.loc[
            data["Date"].dt.strftime("%Y-%W")
            == (pd.to_datetime("now") + dt.timedelta(days=7)).strftime("%Y-%W")
        ]
        to_label_supplementary_data_df = build_to_label_supplementary_data(
            mode_listings
        )
        to_label_supplementary_data_df["mode"] = "next_week"
        to_label_supplementary_data.append(to_label_supplementary_data_df)
        sample_to_label.extend(
            to_label[
                to_label["Normalised Applicant"].isin(
                    mode_listings["Normalised Applicant"].to_list()
                )
            ].index
        )
    return (
        to_label.loc[sample_to_label]
        .merge(pd.concat(to_label_supplementary_data))
        .drop_duplicates(subset=["Normalised Applicant", "mode"])
    )


def add_list_to_case_number(case_number, list_string=""):
    if list_string != "":
        case_number = list_string + " " + case_number
    return case_number


def build_to_label_supplementary_data(
    data, list_max=500, columns=["Cases", "Case Numbers", "Case Names"]
):
    data_frames = []
    latest_data = data.loc[
        data.groupby("Normalised Applicant")["Date"].transform("max") == data["Date"]
    ]
    if "Cases" in columns:
        case_counts = (
            data[["Normalised Applicant", "Case Number"]]
            .drop_duplicates()
            .groupby("Normalised Applicant")
            .nunique()
            .reset_index()
            .rename(columns={"Case Number": "Cases"})
        )
        data_frames.append(case_counts)
    if "Case Numbers" in columns:
        data["Case Number"] = data.apply(
            lambda x: add_list_to_case_number(x["Case Number"], list_string=x["List"]),
            axis=1,
        )
        case_numbers = (
            data[["Normalised Applicant", "Case Number"]]
            .drop_duplicates()
            .groupby("Normalised Applicant")["Case Number"]
            .apply(lambda x: ",".join(list(x)[0:list_max]))
            .reset_index()
            .rename(columns={"Case Number": "Case Numbers"})
        )
        data_frames.append(case_numbers)
    if "Latest Case Number" in columns:
        latest_data["Case Number"] = latest_data.apply(
            lambda x: add_list_to_case_number(x["Case Number"], list_string=x["List"]),
            axis=1,
        )
        latest_case_numbers = latest_data[
            ["Normalised Applicant", "Case Number"]
        ].rename(columns={"Case Number": "Latest Case Number"})
        data_frames.append(latest_case_numbers)
    if "Latest Date" in columns:
        latest_case_dates = latest_data[["Normalised Applicant", "Date"]].rename(
            columns={"Date": "Latest Date"}
        )
        data_frames.append(latest_case_dates)
    if "Latest Case Name" in columns:
        latest_data["Case Name"] = (
            latest_data["Applicant"] + " v " + latest_data["Respondent"]
        )
        latest_case_name = latest_data[["Normalised Applicant", "Case Name"]].rename(
            columns={"Case Name": "Latest Case Name"}
        )
        data_frames.append(latest_case_name)
    if "Case Names" in columns:
        data["Case Name"] = data["Applicant"] + " v " + data["Respondent"]
        case_names = (
            data[["Normalised Applicant", "Case Name"]]
            .drop_duplicates()
            .groupby("Normalised Applicant")["Case Name"]
            .apply(lambda x: ",".join(list(x)[0:list_max]))
            .reset_index()
            .rename(columns={"Case Name": "Case Names"})
        )
        data_frames.append(case_names)
    if "Dates" in columns:
        case_dates = (
            data[["Normalised Applicant", "Date"]]
            .drop_duplicates()
            .groupby("Normalised Applicant")["Date"]
            .apply(lambda x: ",".join(list(x.dt.strftime("%Y-%m-%d"))[0:500]))
            .reset_index()
            .rename(columns={"Date": "Dates"})
        )
        data_frames.append(case_dates)

    df_merged = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Normalised Applicant"], how="outer"
        ),
        data_frames,
    )
    return df_merged


def load_model(model_df, code_lookup_df, selected_cols, true_col):
    log.debug("Loading model from pickled params")
    selected_model_params = load_latest_model_params()
    if selected_model_params is None:
        log.debug("No saved params found — building model")
        return build_model(model_df, code_lookup_df, selected_cols, true_col)
    X_train, X_test, y_train, y_test = train_test_split(
        model_df[selected_cols].iloc[:, :].values,
        model_df[true_col + " Code"].values.ravel(),
        test_size=0.3,
        random_state=42,
    )
    selected_model = RandomForestClassifier(**selected_model_params).fit(
        X_train, y_train
    )

    crdf, key_metrics = produce_classification_report(
        selected_model,
        code_lookup_df,
        X_test,
        y_test,
        true_col,
        return_key_metrics=True,
    )
    log_update_msg = f"Model\n---\n{selected_model.__str__()}\nScoring\n---\n"
    log_update_msg += "".join(
        [f"{key}: {value:f}\n" for key, value in key_metrics.items()]
    )
    log.debug(str(key_metrics))
    return crdf, key_metrics, selected_model, log_update_msg


def build_model(model_df, code_lookup_df, selected_cols, true_col):
    log.debug("Hypertuning Random Forest Model")
    if verbose:
        print("Hypertuning Random Forest Model...")

    X_train, X_test, y_train, y_test = train_test_split(
        model_df[selected_cols].iloc[:, :].values,
        model_df[true_col + " Code"].values.ravel(),
        test_size=0.3,
        random_state=42,
    )
    clf = hypertune_model(X_train, y_train)

    if verbose:
        msg = "Best Model Params\n" + f"{clf['randomizedsearchcv'].best_params_}"
        print(msg)
    log.info(msg)

    selected_model = RandomForestClassifier(
        **clf["randomizedsearchcv"].best_params_
    ).fit(X_train, y_train)
    pkl_model(selected_model)
    pkl_params(clf["randomizedsearchcv"].best_params_)

    crdf, key_metrics = produce_classification_report(
        selected_model,
        code_lookup_df,
        X_test,
        y_test,
        true_col,
        return_key_metrics=True,
    )
    log_update_msg = f"Model\n---\n{selected_model.__str__()}\nScoring\n---\n"
    log_update_msg += "".join(
        [f"{key}: {value:f}\n" for key, value in key_metrics.items()]
    )
    log.debug(str(key_metrics))
    return crdf, key_metrics, selected_model, log_update_msg


def _format_crdf_slack(crdf, max_chars: int) -> str:
    display = crdf.copy()
    display.index = [
        name if len(name) <= max_chars else name[: max_chars - 3] + "..."
        for name in display.index
    ]
    return "```\n" + display.to_string(float_format="{:.2f}".format) + "\n```"


def run() -> str:
    t0 = time.monotonic()
    mode = ["month", "all"]
    max_labels = 50

    applicant_labels = ls.load_applicant_labels(set_vars=True)
    data = ls.load_listings(update_labels=True, applicant_labels=applicant_labels)
    features_df = ls.load_features(
        update_labels=True, applicant_labels=applicant_labels
    )

    with profile_resources("build_bag_of_words_features"):
        train_data_features, bag_of_words_cols = build_bag_of_words_features(
            features_df, max_features=5000
        )
    features_df = features_df.merge(train_data_features, how="left")
    features_df = features_df.merge(build_list_total_features(data), how="left")
    selected_cols = feature_cols(features_df, bag_of_words_cols)
    features_df, model_df, code_lookup_df = ls.reduce_applicant_labels(
        features_df, applicant_labels, data, max_labels=max_labels
    )

    true_col = "Reduced Applicant Label"
    pred_col = f"{true_col} Prediction"

    crdf, key_metrics, selected_model, _ = load_model(
        model_df, code_lookup_df, selected_cols, true_col
    )
    y_pred = make_predictions(
        features_df[selected_cols].iloc[:, :].values, selected_model
    )

    to_label = build_to_label(
        features_df,
        y_pred,
        code_lookup_df,
        pred_col,
        true_col,
        mode=mode,
        data=data,
    )
    ls.save_to_label(to_label, split_save="mode")

    elapsed = round(time.monotonic() - t0)
    summary = (
        f"prepare_to_label: {len(features_df)} applicants processed, "
        f"{elapsed}s elapsed"
    )
    log.success(summary)
    crdf_block = _format_crdf_slack(crdf, constants.SLACK_MAX_PROVIDER_CHARS)
    return summary + "\n" + crdf_block


if __name__ == "__main__":

    try:
        # Settings
        verbose = True
        build_data_bool = True
        load_model_bool = True
        make_predictions_bool = True
        # Labelling settings
        # mode = ["week", "month", "today", "random_label"]
        mode = ["month", "all"]
        max_labels = 50
        log_update_msg = ""

        if build_data_bool:
            # Building Data
            (
                data,
                applicant_labels,
                features_df,
                model_df,
                code_lookup_df,
                selected_cols,
            ) = build_data(max_labels=max_labels)
        # exit(0)
        # Setting Column Names
        original_col = "Applicant Label"
        true_col = f"Reduced {original_col}"
        pred_col = f"{true_col} Prediction"

        if load_model_bool:
            crdf, key_metrics, selected_model, log_update_msg = load_model(
                model_df, code_lookup_df, selected_cols, true_col
            )
        else:
            crdf, key_metrics, selected_model, log_update_msg = build_model(
                model_df, code_lookup_df, selected_cols, true_col
            )

        if make_predictions_bool:
            y_pred = make_predictions(
                features_df[selected_cols].iloc[:, :].values, selected_model
            )

            to_label = build_to_label(
                features_df,
                y_pred,
                code_lookup_df,
                pred_col,
                true_col,
                mode=mode,
                data=data,
            )
            ls.save_to_label(to_label, split_save="mode")
        log.info("prepare_to_label.py completed")

    except Exception as e:
        log.error("CRITICAL ERROR\n" + str(e) + "\n" + format_traceback())
        post_log_update(
            "CRITICAL ERROR\n" + f"{Path(__file__).as_posix()}\n" + format_traceback()
        )
        sys.exit(e)
