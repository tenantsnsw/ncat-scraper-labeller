import pandas as pd

import ast
import datetime as dt
from sklearn.preprocessing import LabelEncoder
import constants

bool_replace_dict = constants.BOOL_REPLACE_DICT

# General Methods


def remove_empty_cols(df, ignore_cols=[]):
    # Remove empty cols
    for col in [col for col in df.columns if col not in ignore_cols]:
        if df[col].isna().all():
            df = df.drop(columns=col)
    return df


def load_entity_classification(entities):
    if isinstance(entities, str):
        if entities == "":
            return pd.NA
        else:
            return entities.split(",")
    else:
        return pd.NA


def save_entity_classification(entities):
    if isinstance(entities, list):
        return ",".join(entities)
    else:
        return ""


def load_list_col(x, col):
    if x[col] == "":
        return pd.NA
    else:
        return ast.literal_eval(x[col])


def set_social_housing_provider_bool(applicant_labels):
    applicant_labels["Social Housing Provider"] = pd.NA
    applicant_labels.loc[
        applicant_labels["Applicant Label"] == "NOT A SOCIAL HOUSING PROVIDER",
        "Social Housing Provider",
    ] = False
    applicant_labels.loc[
        (applicant_labels["Applicant Label"] != "NOT A SOCIAL HOUSING PROVIDER")
        & (applicant_labels["Applicant Label"] != ""),
        "Social Housing Provider",
    ] = True
    return applicant_labels


# Listings Methods


def load_listings(update_labels=False, applicant_labels=None):
    dtypes = {
        "UID": "string",
        "Case Number": "string",
        "List": "string",
        "Listing Type": "string",
        "Address": "string",
        "Presiding Officers": "string",
        "Location": "string",
        "Court Room": "string",
        "Time": "string",
        "Applicant": "string",
        "Respondent": "string",
    }
    data = pd.read_csv(
        constants.PROCESSED_DATA_DIR / "listings.csv",
        parse_dates=["Date"],
        dtype=dtypes,
        keep_default_na=False,
        low_memory=False,
    )

    if update_labels:
        if isinstance(applicant_labels, pd.DataFrame):
            data = update_data_labels(data, applicant_labels)
        else:
            data = update_data_labels(data, load_applicant_labels())
    else:
        data["Social Housing Provider"] = (
            data["Social Housing Provider"].replace(bool_replace_dict).astype("boolean")
        )
        data["Entity Classification"] = data["Entity Classification"].apply(
            load_entity_classification
        )
    data["Applicant Label"] = data["Applicant Label"].fillna("").astype(str)
    return data


def save_listings(data):
    data["Entity Classification"] = data["Entity Classification"].apply(
        save_entity_classification
    )
    data.sort_values("Date", ascending=False).to_csv(
        constants.PROCESSED_DATA_DIR / "listings.csv", index=False
    )


def update_data_labels(data, applicant_labels):
    labeled_cols = [
        "Applicant Label",
        "Social Housing Provider",
        "Entity Classification",
    ]
    data = data[[col for col in data.columns if col not in labeled_cols]].merge(
        applicant_labels[applicant_labels["Normalised Applicant"] != ""][
            ["Normalised Applicant"] + labeled_cols
        ],
        how="left",
    )

    return data


# Applicant Label Methods
def load_applicant_labels(
    filename="",
    set_vars=False,
    recalc_applications=False,
    data=None,
    providers=None,
    backup=False,
):
    """
    Load the saved applicant labels from the GDrive
    """
    if filename == "":
        applicant_labels = pd.read_csv(
            constants.PROCESSED_DATA_DIR / "applicant_labels.csv", low_memory=False
        )
    else:
        applicant_labels = pd.read_csv(filename, low_memory=False)
    applicant_labels = applicant_labels.rename(
        columns={"Applicant": "Normalised Applicant"}
    )
    applicant_labels["Applicant Label"] = (
        applicant_labels["Applicant Label"].fillna("").astype(str)
    )
    applicant_labels["Entity Classification"] = applicant_labels[
        "Entity Classification"
    ].apply(load_entity_classification)
    applicant_labels["Social Housing Provider"] = (
        applicant_labels["Social Housing Provider"]
        .replace(bool_replace_dict)
        .astype("boolean")
    )
    if backup:
        backup_applicant_labels(applicant_labels)
    if set_vars:
        if not isinstance(providers, pd.DataFrame):
            providers = load_providers()
        applicant_labels["Official Provider"] = applicant_labels[
            "Applicant Label"
        ].apply(lambda x: pd.NA if x == "" else x in list(providers["Provider Group"]))
        applicant_labels = set_social_housing_provider_bool(applicant_labels)
    if recalc_applications:
        if isinstance(data, pd.DataFrame):
            pd.DataFrame(
                data[data["List"].apply(lambda x: x in ["SH", ""])][
                    "Normalised Applicant"
                ].value_counts()
            ).reset_index().rename(columns={"count": "Applications"}).merge(
                applicant_labels[
                    [col for col in applicant_labels.columns if col != "Applications"]
                ],
                how="left",
            )
        else:
            return load_applicant_labels(
                set_vars=set_vars,
                recalc_applications=recalc_applications,
                data=load_listings(),
            )
    return applicant_labels


def save_applicant_labels(
    applicant_labels,
    providers=None,
    set_vars=True,
    backup=True,
):
    if not isinstance(providers, pd.DataFrame):
        providers = load_providers()
    applicant_labels = applicant_label_column_saving(applicant_labels)
    if set_vars:
        applicant_labels["Official Provider"] = applicant_labels[
            "Applicant Label"
        ].apply(lambda x: pd.NA if x == "" else x in list(providers["Provider Group"]))
        applicant_labels = set_social_housing_provider_bool(applicant_labels)
    if backup:
        backup_applicant_labels(applicant_labels)

    applicant_labels.to_csv(
        constants.PROCESSED_DATA_DIR / "applicant_labels.csv", index=False
    )


def update_applicant_labels_from_listings(
    applicant_labels=None,
    listings=None,
    backup=False,
    recalc_applications=True,
):
    """
    Update the applicant labels from the listings data.
    If applicant_labels is None, load the applicant labels from the GDrive.
    If listings is None, load the listings data from the GDrive.
    If backup is True, backup the applicant labels before updating.
    """
    if applicant_labels is None:
        applicant_labels = load_applicant_labels(backup=backup)
    if backup:
        backup_applicant_labels(applicant_labels)
    if listings is None:
        listings = load_listings(update_labels=False)
    applicant_labels = (
        listings[["Normalised Applicant"]]
        .drop_duplicates()
        .merge(applicant_labels, how="left", on="Normalised Applicant")
    )
    if recalc_applications:
        pd.DataFrame(
            listings[listings["List"].apply(lambda x: x in ["SH", ""])][
                "Normalised Applicant"
            ].value_counts()
        ).reset_index().rename(columns={"count": "Applications"}).merge(
            applicant_labels[
                [col for col in applicant_labels.columns if col != "Applications"]
            ],
            how="left",
        )
    return applicant_labels


def update_applicant_labels(applicant_labels=None, labelled=None, backup=False):
    if applicant_labels is None:
        applicant_labels = load_applicant_labels(backup=backup)
    elif backup:
        backup_applicant_labels(applicant_labels)
    if labelled is None:
        labelled = load_to_label_excel()
    if isinstance(labelled, dict):
        # labelled_list = []
        for sheet_name, to_label_sheet in labelled.items():
            applicant_labels = update_applicant_labels(
                applicant_labels=applicant_labels, labelled=to_label_sheet
            )
    else:
        labelled = labelled.set_index("Normalised Applicant")
        applicant_labels = applicant_labels.set_index("Normalised Applicant")
        applicant_labels.update(
            labelled[
                list(set(labelled.columns).intersection(set(applicant_labels.columns)))
            ]
        )
        applicant_labels = applicant_labels.reset_index()
        # Only include labelled data
        labelled = labelled.reset_index()
        applicant_labels = (
            pd.concat(  # Add in the applicants in labelled not in applicant_labels
                [
                    applicant_labels,
                    labelled.loc[
                        ~labelled["Normalised Applicant"].isin(
                            applicant_labels["Normalised Applicant"].to_list()
                        ),
                        list(
                            set(labelled.columns).intersection(
                                set(applicant_labels.columns)
                            )
                        ),
                    ],
                ]
            )
        )
    return applicant_labels


def backup_applicant_labels(applicant_labels):
    applicant_labels = applicant_label_column_saving(applicant_labels)
    now = dt.datetime.today().strftime("%Y%m%d%H%M%S")
    applicant_labels.to_csv(
        constants.PROCESSED_DATA_DIR / "backups" / f"applicant_labels{now}.csv",
        index=False,
    )


def applicant_label_column_saving(applicant_labels):
    applicant_labels = applicant_labels.rename(
        columns={"Normalised Applicant": "Applicant"}
    )
    applicant_labels["Entity Classification"] = applicant_labels[
        "Entity Classification"
    ].apply(save_entity_classification)
    return applicant_labels


# Providers Methods


def load_providers(filename="", backup=False):
    if filename == "":
        providers = pd.read_csv(constants.PROCESSED_DATA_DIR / "providers.csv")
    else:
        providers = pd.read_csv(filename)
    providers = providers_column_loading(providers)
    if backup:
        now = dt.datetime.today().strftime("%Y%m%d%H%M%S")
        providers.to_csv(
            constants.PROCESSED_DATA_DIR / "backups" / f"providers{now}.csv",
            index=False,
        )
    return providers


def save_providers(providers, backup=False):
    providers = providers_column_saving(providers)
    providers.to_csv(constants.PROCESSED_DATA_DIR / "providers.csv", index=False)
    if backup:
        now = dt.datetime.today().strftime("%Y%m%d%H%M%S")
        providers.to_csv(
            constants.PROCESSED_DATA_DIR / "backups" / f"providers{now}.csv",
            index=False,
        )


def providers_column_loading(providers):
    bool_cols = ["Aboriginal Provider", "AHO Registered", "LALC"]
    for bool_col in bool_cols:
        providers[bool_col] = providers[bool_col].astype(
            "boolean"
        )  # Nullable boolean type
    return providers


def providers_column_saving(providers):
    bool_cols = providers.select_dtypes(include=["bool", "boolean"]).columns
    for bool_col in bool_cols:
        providers[bool_col] = providers[bool_col].astype(str).replace("<NA>", "")
    return providers


# NRSCH Data


def load_nrsch_data(original=False):
    nrsch_data = pd.read_csv(constants.LATEST_NRSCH_DATA)
    if original:
        return nrsch_data
    # Select NSW
    nrsch_data = nrsch_data[
        nrsch_data["Primary Jurisdiction"].str.contains("New South Wales")
        | nrsch_data["Other Jurisdictions"].str.contains("New South Wales")
    ]
    nrsch_data["Provider Type"] = "NRSCH"
    nrsch_data = nrsch_data.rename(columns={"Provider Name": "Provider Official Name"})
    return nrsch_data


def new_providers(providers, nrsch_data=None):
    if nrsch_data is None:
        nrsch_data = load_nrsch_data()
    empty_providers = pd.DataFrame(columns=providers.columns)
    shared_cols = [col for col in providers.columns if col in nrsch_data.columns]
    nrsch_data = nrsch_data.loc[
        ~nrsch_data["Registration Number"].isin(providers["Registration Number"]),
        shared_cols,
    ]
    return pd.concat([empty_providers, nrsch_data])


def update_providers(providers, nrsch_data=None):
    if nrsch_data is None:
        nrsch_data = load_nrsch_data()
    new_providers_df = new_providers(providers, nrsch_data=nrsch_data)
    new_providers_df = new_providers_df.loc[:, (~new_providers_df.isna()).sum() != 0]
    providers = pd.concat([providers, new_providers_df])
    providers = providers.set_index("Registration Number")
    nrsch_data = nrsch_data.set_index("Registration Number")
    providers.update(nrsch_data)
    providers = providers.reset_index()
    # Sum together Total Community Housing Assets over a Provider Group
    if "Provider Group Total Dwellings" in providers.columns:
        providers = providers.drop(columns="Provider Group Total Dwellings")
    providers = providers.merge(
        (
            providers.loc[
                providers["Total Community Housing Assets"] > 0,
                ["Provider Group", "Total Community Housing Assets"],
            ]
            .rename(
                columns={
                    "Total Community Housing Assets": "Provider Group Total Dwellings"
                }
            )
            .groupby("Provider Group")
            .sum()
            .reset_index()
        ),
        how="left",
    )
    return providers


# To Label Methods


def load_to_label(filename=""):
    if filename == "":
        to_label = pd.read_csv(constants.PROCESSED_DATA_DIR / "to_label.csv")
    else:
        to_label = pd.read_csv(filename)

    to_label = to_label_column_loading(to_label)
    return to_label


def load_to_label_excel(filename=""):
    # Load Excel
    if filename == "":
        with pd.ExcelFile(
            constants.PROCESSED_DATA_DIR / "Social Housing NCAT Labelling.xlsx"
        ) as xls:
            excel_sheets = pd.read_excel(xls, None)
    else:
        with pd.ExcelFile(filename) as xls:
            excel_sheets = pd.read_excel(xls, None)
    # Find all sheets with To Label in them
    to_label = {}
    for sheet_name, sheet in excel_sheets.items():
        if "to label" in sheet_name.lower():
            to_label[sheet_name] = sheet
    # Load columns properly
    for sheet_name, sheet in to_label.items():
        to_label[sheet_name] = to_label_column_loading(sheet)
    if len(to_label) == 1:
        to_label = to_label[list(to_label.keys())[0]]
    return to_label


def save_to_label(to_label, split_save=None):
    if isinstance(to_label, dict):
        for sheet_name, to_label_sheet in to_label.items():
            save_to_label(to_label_sheet, split_save=split_save)
    else:
        to_label = to_label_column_saving(to_label)
        if split_save is not None:
            for split in to_label[split_save].unique():
                to_label_split = to_label[to_label[split_save] == split]
                remove_empty_cols(
                    to_label_split,
                    # Don't remove these cols in the rare case they are empty
                    ignore_cols=[
                        "Applicant Label",
                        "Entity Classification",
                        "Social Housing Provider",
                    ],
                ).to_csv(
                    constants.PROCESSED_DATA_DIR / f"to_label_{split}.csv",
                    index=False,
                )
        else:
            remove_empty_cols(
                to_label,
                ignore_cols=[  # Don't remove these cols in the rare case they are empty
                    "Applicant Label",
                    "Entity Classification",
                    "Social Housing Provider",
                ],
            ).to_csv(constants.PROCESSED_DATA_DIR / "to_label.csv", index=False)


def to_label_column_saving(to_label, keep=[]):
    if "Normalised Applicant" not in keep:
        to_label = to_label.rename(columns={"Normalised Applicant": "Applicant"})
    if "Reduced Applicant Label Prediction" not in keep:
        to_label = to_label.rename(
            columns={
                "Reduced Applicant Label Prediction": "Applicant Label Prediction",
            }
        )
    if "Entity Classification" not in keep:
        to_label["Entity Classification"] = to_label["Entity Classification"].apply(
            save_entity_classification
        )
    if "Social Housing Provider" not in keep:
        to_label["Social Housing Provider"] = (
            to_label["Social Housing Provider"].astype(str).replace("<NA>", "")
        )
    to_label = to_label.sort_values("Applicant Label Prediction")
    to_label_provider = to_label[
        ~to_label["Applicant Label Prediction"].isin(
            ["NOT A SOCIAL HOUSING PROVIDER", "Other"]
        )
    ]
    to_label_other = to_label[to_label["Applicant Label Prediction"] == "Other"]
    to_lobal_not_provider = to_label[
        to_label["Applicant Label Prediction"] == "NOT A SOCIAL HOUSING PROVIDER"
    ]
    to_label = pd.concat([to_label_provider, to_label_other, to_lobal_not_provider])
    return to_label


def to_label_column_loading(to_label):

    to_label = to_label.rename(
        columns={
            "Applicant": "Normalised Applicant",
            "Applicant Label Prediction": "Reduced Applicant Label Prediction",
        }
    )
    to_label["Applicant Label"] = to_label["Applicant Label"].fillna("").astype(str)
    if "Entity Classification" in to_label.columns:
        to_label["Entity Classification"] = to_label["Entity Classification"].apply(
            load_entity_classification
        )
    if "Social Housing Provider" in to_label.columns:
        to_label["Social Housing Provider"] = (
            to_label["Social Housing Provider"]
            .replace(bool_replace_dict)
            .astype("boolean")
        )
    return to_label


# Features Methods


def load_jaccard_features(update_labels=True, applicant_labels=None, verbose=True):
    if verbose:
        print("Loading Jaccard Features...")
    jaccard_features = pd.read_csv(
        constants.PROCESSED_DATA_DIR / "jaccard_features.csv"
    )
    if update_labels:
        if not isinstance(applicant_labels, pd.DataFrame):
            applicant_labels = load_applicant_labels(set_vars=True)
        cols_to_merge = [
            "Normalised Applicant",
            "Applicant Label",
            "Entity Classification",
            "Social Housing Provider",
        ]
        jaccard_features = jaccard_features.merge(
            applicant_labels[cols_to_merge], how="left"
        )
        jaccard_features["Applicant Label"] = (
            jaccard_features["Applicant Label"].fillna("").astype(str)
        )
        jaccard_features = jaccard_features[
            (jaccard_features["Normalised Applicant"].apply(type) == str)
        ]
    return jaccard_features


def load_features(update_labels=True, applicant_labels=None, verbose=True):
    if verbose:
        print("Loading Features...")
    jaccard_features = load_jaccard_features(
        update_labels=True, applicant_labels=applicant_labels
    )
    features_df = jaccard_features
    del jaccard_features
    features_df = features_df[(features_df["Normalised Applicant"].apply(type) == str)]
    features_df = features_df.rename(
        columns={col: "UNLABELED" for col in features_df.columns if "Unnamed" in col}
    )
    if verbose:
        print("\nUnique Labelled Features:\n")
        for col in [
            "Applicant Label",
            "Entity Classification",
            "Social Housing Provider",
        ]:
            print(col, ((features_df[col] != "") & (~features_df[col].isnull())).sum())
        print("\n")
    return features_df


def reduce_applicant_labels(
    features_df, data=None, applicant_labels=None, max_labels=30
):

    def reduce_applicant_label(applicant_label):
        if applicant_label in top_applicants:
            return applicant_label
        else:
            return "Other"

    if data is None:
        data = load_listings(update_labels=False, applicant_labels=applicant_labels)
    top_applicants = pd.DataFrame(
        data.merge(
            applicant_labels.loc[
                applicant_labels["Applicant Label"] != "",
                ["Normalised Applicant", "Applicant Label"],
            ]
        )[["Applicant Label"]].value_counts()
    ).reset_index()
    if "NOT A SOCIAL HOUSING PROVIDER" in top_applicants["Applicant Label"].unique():
        max_labels += 1
    top_applicants = list(top_applicants[0:max_labels]["Applicant Label"])

    features_df = features_df.copy()
    features_df["Reduced Applicant Label"] = features_df["Applicant Label"].apply(
        reduce_applicant_label
    )

    features_df["Reduced Applicant Label Code"] = LabelEncoder().fit_transform(
        features_df["Reduced Applicant Label"]
    )
    model_df = features_df.loc[features_df["Applicant Label"] != "", :]
    code_lookup_df = (
        model_df[["Reduced Applicant Label", "Reduced Applicant Label Code"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    code_lookup_df = code_lookup_df.set_index("Reduced Applicant Label Code")
    return features_df, model_df, code_lookup_df
