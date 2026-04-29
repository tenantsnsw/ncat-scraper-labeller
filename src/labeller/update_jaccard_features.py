import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from datasketch import MinHash, MinHashLSH
from nltk import ngrams
import constants
import pandas as pd
import pickle
import time

from tqdm import tqdm
import label_loading_saving as ls
from core.logger_config import get_logger, post_log_update, format_traceback
from core.profiler import profile_resources

log = get_logger(__file__)

# from tqdm.auto import tqdm  # for notebooks
tqdm.pandas()

# unimportant_words = ["And", "Limited", "Proprietary", "The", "As", "For"]
unimportant_words = []
if (constants.PKL_DIR / "minhashes.pkl").is_file():
    with open(constants.PKL_DIR / "minhashes.pkl", "rb") as f:
        minhashes = pickle.load(f)
else:
    minhashes = {}
pd.options.mode.copy_on_write = True


def build_minhash_pkls(
    unique_applicants,
    thresholds,
    lshs=None,
    load_hash=True,
    clear_memory=True,
    rebuild_minhash=False,
    return_lshs_dict=True,
    unimportant_words=unimportant_words,
):
    # Get rid of the unimportant words
    unique_applicants = unique_applicants.apply(
        lambda x: " ".join(
            [word for word in x.split() if word not in unimportant_words]
        )
    )
    unique_applicants = unique_applicants.drop_duplicates()
    if isinstance(thresholds, float):
        thresholds = [thresholds]
    minhashes_change = False
    if not isinstance(lshs, dict):
        lshs = {str(int(threshold * 100)): None for threshold in thresholds}
    for threshold in thresholds:
        threshold_str = str(int(threshold * 100))
        lsh_path = constants.PKL_DIR / f"lsh_{threshold_str}.pkl"
        if lsh_path.is_file() and not rebuild_minhash:
            if load_hash:
                with open(lsh_path, "rb") as f:
                    lshs[threshold_str] = pickle.load(f)
        else:
            lsh_x = MinHashLSH(threshold=threshold, num_perm=128)
            # Create MinHash objects
            log.info("Building MinHash " + threshold_str)

            for idx, applicant in tqdm(
                unique_applicants.items(),
                total=len(unique_applicants),
            ):
                minhash_x = MinHash(num_perm=128)
                for d in ngrams(applicant, 3):
                    minhash_x.update("".join(d).encode("utf-8"))
                if applicant not in minhashes.keys():
                    minhashes[applicant] = minhash_x
                    minhashes_change = True
                lsh_x.insert(applicant, minhash_x)
            with open(lsh_path, "wb") as f:
                pickle.dump(lsh_x, f)
            if minhashes_change:
                with open(constants.PKL_DIR / "minhashes.pkl", "wb") as f:
                    pickle.dump(minhashes, f)
            lshs[threshold_str] = lsh_x
            if clear_memory:
                del minhash_x, lsh_x
    if return_lshs_dict:
        return lshs
    else:
        return None


def rebuild_minhashes(unique_applicants, overwrite=True, unimportant_words=[]):
    minhashes = {}
    unique_applicants = unique_applicants.apply(
        lambda x: " ".join(
            [word for word in x.split() if word not in unimportant_words]
        )
    )
    unique_applicants = unique_applicants.drop_duplicates()
    for idx, applicant in tqdm(unique_applicants.items(), total=len(unique_applicants)):
        minhash_x = MinHash(num_perm=128)
        for d in ngrams(applicant, 3):
            minhash_x.update("".join(d).encode("utf-8"))
        if applicant not in minhashes.keys():
            minhashes[applicant] = minhash_x
    if overwrite:
        with open(constants.PKL_DIR / "minhashes.pkl", "wb") as f:
            pickle.dump(minhashes, f)
    return minhashes


def tidy_minhashes(unique_applicants, minhashes, overwrite=True, unimportant_words=[]):
    unique_applicants = unique_applicants.apply(
        lambda x: " ".join(
            [word for word in x.split() if word not in unimportant_words]
        )
    )
    unique_applicants = unique_applicants.drop_duplicates()
    new_minhashes = {}
    for idx, applicant in tqdm(unique_applicants.items(), total=len(unique_applicants)):
        try:
            new_minhashes[applicant] = minhashes[applicant]
        except KeyError:
            pass
    if overwrite:
        with open(constants.PKL_DIR / "minhashes.pkl", "wb") as f:
            pickle.dump(new_minhashes, f)
    return new_minhashes


def load_minhash(x: str):
    lsh_pkl = constants.PKL_DIR / f"lsh_{x}.pkl"
    if lsh_pkl.is_file():
        try:
            with open(lsh_pkl, "rb") as f:
                lsh_x = pickle.load(f)
            f.close()
            return lsh_x
        except Exception as e:
            print("Exception loading MinHashLSH object", "\n" + "Exception", e)
    else:
        print("No lsh_pkl object", lsh_pkl)
        return None


def update_minhash(to_update, lsh_x, minhashes, save_to_pickle=True, lsh_pkl=""):
    minhashes_keys = list(minhashes.keys())
    minhashes_change = False
    for idx, applicant in tqdm(to_update.items(), total=len(to_update)):
        try:
            minhash_x = MinHash(num_perm=128)
            for d in ngrams(applicant, 3):
                minhash_x.update("".join(d).encode("utf-8"))
            try:  # If applicant is already in the minhash, delete it and replace.
                lsh_x.insert(applicant, minhash_x)
            except ValueError as ve:
                if ve.args[0] == "The given key already exists":
                    lsh_x.remove(applicant)
                    lsh_x.insert(applicant, minhash_x)
                else:
                    raise ve
            if applicant not in minhashes_keys:
                minhashes[applicant] = minhash_x
                minhashes_change = True
        except Exception as e:
            print(idx, applicant)
            raise Exception(e)

    if save_to_pickle:
        with open(lsh_pkl, "wb") as f:  # open a text file
            pickle.dump(lsh_x, f)  # serialize the list
        if minhashes_change:
            with open(constants.PKL_DIR / "minhashes.pkl", "wb") as f:
                pickle.dump(minhashes, f)
    return lsh_x, minhashes


def query_minhashLSH(
    applicant, lsh, minhashes=minhashes, unimportant_words=unimportant_words
):
    new_word = " ".join(
        [word for word in applicant.split() if word not in unimportant_words]
    )
    try:
        query = minhashes[new_word]
        return lsh.query(query), query
    except KeyError as ke:
        print(ke, "Key Error")
        return [], None


def jaccard_tuples(
    applicant, lsh, minhashes=minhashes, max=100, unimportant_words=unimportant_words
):
    results, query = query_minhashLSH(
        applicant, lsh, minhashes=minhashes, unimportant_words=unimportant_words
    )
    jaccard_tuples_list = []
    if len(results) == 0:
        return []
    else:
        for result in results:
            try:
                jaccard_tuples_list.append((query.jaccard(minhashes[result]), result))
            except KeyError as ke:
                print(applicant)
                print(result, ke)
        jaccard_tuples_list.sort(reverse=True)
        jaccard_tuples_list.remove(jaccard_tuples_list[0])
        return jaccard_tuples_list[0:max]


def build_lookup_df(data=None, unimportant_words=unimportant_words):
    if not isinstance(data, pd.DataFrame):
        data = ls.load_listings(update_labels=True)
    lookup_df = data[["Normalised Applicant", "Applicant Label"]]
    lookup_df["Normalised Applicant"] = lookup_df["Normalised Applicant"].apply(
        lambda x: " ".join(
            [word for word in x.split() if word not in unimportant_words]
        )
    )
    lookup_df = lookup_df.drop_duplicates()

    lookup_df_duplicated = lookup_df[
        lookup_df.duplicated(subset="Normalised Applicant", keep=False)
    ]
    lookup_df_unduplicated = lookup_df[
        ~lookup_df.duplicated(subset="Normalised Applicant", keep=False)
    ]
    lookup_df_duplicated = lookup_df_duplicated[
        lookup_df_duplicated["Applicant Label"] != ""
    ]
    lookup_df = pd.concat([lookup_df_unduplicated, lookup_df_duplicated])
    return lookup_df.set_index("Normalised Applicant")


def sum_label_score(
    applicant,
    lsh,
    lookup_df,
    minhashes,
    results,
    max=100,
    unimportant_words=unimportant_words,
):
    try:
        score_dict = {}
        for result in results:
            try:
                app_label = lookup_df.loc[result[1], "Applicant Label"]
                if app_label == "":
                    app_label = "UNLABELED"
                app_label = app_label + " Sum Score"
                if app_label in score_dict.keys():
                    score_dict[app_label] += result[0]
                else:
                    score_dict[app_label] = result[0]
            except KeyError as ke:
                print(ke, "KeyError")
        return score_dict
    except Exception as e:
        print("Exception in sum_label_score", "\n" + "Exception", e)
        local_variables = locals()
        for variable_name, variable in local_variables.items():
            if variable_name not in ["lsh_50", "minhashes"]:
                print(variable_name, "=", local_variables[variable_name])
        raise e


def average_label_score(
    applicant,
    lsh,
    lookup_df,
    minhashes,
    results,
    max=100,
    unimportant_words=unimportant_words,
):

    score_dict = {}
    for result in results:
        try:
            app_label = lookup_df.loc[result[1], "Applicant Label"]
            if app_label == "":
                app_label = "UNLABELED"
            app_label = app_label + " Average Score"
            if app_label in score_dict.keys():
                score_dict[app_label] += result[0]
            else:
                score_dict[app_label] = result[0]
        except KeyError as ke:
            print(ke, "KeyError")
    total = 0
    for app_label, score in score_dict.items():
        if "UNLABELED" not in app_label:
            total += score
    score_dict = {
        app_label: score_dict[app_label] / total
        for app_label in score_dict.keys()
        if "UNLABELED" not in app_label
    }
    return score_dict


def run() -> str:
    global minhashes
    t0 = time.monotonic()

    with profile_resources("load_listings"):
        data = ls.load_listings(update_labels=True)
    unique_applicants = data["Normalised Applicant"].drop_duplicates()
    n_applicants = len(unique_applicants)
    log.info(f"Loaded {n_applicants} unique applicants")

    jaccard_features = pd.DataFrame(unique_applicants)

    to_update = unique_applicants.apply(
        lambda x: " ".join(
            [w for w in x.split() if w not in unimportant_words]
        )
    )
    to_update = to_update[
        ~to_update.apply(lambda x: x in minhashes.keys())
    ]
    minhashes = tidy_minhashes(
        unique_applicants,
        minhashes,
        overwrite=False,
        unimportant_words=unimportant_words,
    )

    with profile_resources("build_minhash_pkls"):
        lshs = build_minhash_pkls(
            unique_applicants, 0.5, rebuild_minhash=True, return_lshs_dict=True
        )
    lsh_50 = lshs["50"]

    with profile_resources("load_listings (lookup)"):
        listings_for_lookup = ls.load_listings(
            update_labels=True, applicant_labels=None
        )
    with profile_resources("build_lookup_df"):
        lookup_df = build_lookup_df(
            listings_for_lookup, unimportant_words=unimportant_words
        )

    log.info("Building Jaccard Tuples")
    with profile_resources("jaccard_tuples"):
        jaccard_features["Jaccard Tuples"] = jaccard_features[
            "Normalised Applicant"
        ].progress_apply(
            lambda x: jaccard_tuples(
                x,
                lsh_50,
                minhashes=minhashes,
                max=10,
                unimportant_words=unimportant_words,
            )
        )

    log.info("Building Label Sum Score")
    with profile_resources("sum_label_score"):
        jaccard_features["Label Sum Score"] = jaccard_features.progress_apply(
            lambda x: sum_label_score(
                x["Normalised Applicant"],
                lsh_50,
                lookup_df,
                minhashes,
                x["Jaccard Tuples"],
                unimportant_words=unimportant_words,
            ),
            axis=1,
        )

    log.info("Building Label Average Score")
    with profile_resources("average_label_score"):
        jaccard_features["Label Average Score"] = jaccard_features.progress_apply(
            lambda x: average_label_score(
                x["Normalised Applicant"],
                lsh_50,
                lookup_df,
                minhashes,
                x["Jaccard Tuples"],
                unimportant_words=unimportant_words,
            ),
            axis=1,
        )

    jaccard_features = jaccard_features.reset_index(drop=True).join(
        pd.json_normalize(jaccard_features["Label Sum Score"])
        .fillna(0)
        .join(
            pd.json_normalize(jaccard_features["Label Average Score"]).fillna(0)
        )
    )

    out_path = constants.PROCESSED_DATA_DIR / "jaccard_features.csv"
    jaccard_features.to_csv(out_path, index=False)

    elapsed = round(time.monotonic() - t0)
    summary = (
        f"update_jaccard_features: {n_applicants} applicants, {elapsed}s elapsed"
    )
    log.success(summary)
    return summary


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        log.error("CRITICAL ERROR\n" + str(e) + "\n" + format_traceback())
        post_log_update(
            "CRITICAL ERROR\n"
            + f"{Path(__file__).as_posix()}\n"
            + format_traceback()
        )
        sys.exit(e)
