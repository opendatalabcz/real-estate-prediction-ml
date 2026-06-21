"""
Generate all 18 figures for the thesis report (report.tex).
Output: PNG + PDF in NBS/images/
"""

from pathlib import Path
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats
from sklearn.neighbors import BallTree

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
ARTIFACTS_DIR = REPO_ROOT / "artifacts" / "poi_models"
MODELS_DIR = REPO_ROOT / "models"
IMG_DIR = REPO_ROOT / "NBS" / "images"
IMG_DIR.mkdir(parents=True, exist_ok=True)

TEXT_WIDTH_CM = 16
FULL = TEXT_WIDTH_CM / 2.54
HALF = FULL / 2

DPI = 300

# ---------------------------------------------------------------------------
# 0.  Global figure context
# ---------------------------------------------------------------------------

sns.set_theme(style="ticks", context="paper", font_scale=0.9, palette="colorblind")
plt.rcParams.update({
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.1,
    "font.family": "sans-serif",
    "font.sans-serif": ["Arial", "DejaVu Sans"],
    "mathtext.fontset": "stix",
})
GREYS = sns.color_palette("Greys", n_colors=9)
COLOR_PAL = sns.color_palette("colorblind")


# ---------------------------------------------------------------------------
# 1.  Data loading
# ---------------------------------------------------------------------------

def load_data():
    df = pd.read_csv(DATA_DIR / "apartments_raw_data.csv")
    region_map = pd.read_csv(DATA_DIR / "region_mapping.csv")
    baseline_metrics = pd.read_csv(ARTIFACTS_DIR / "reference_baseline_metrics.csv")
    final_test = pd.read_csv(ARTIFACTS_DIR / "final_test_comparison_locked_variants.csv")
    feat_imp = pd.read_csv(ARTIFACTS_DIR / "locked_xgb_feature_importances.csv")
    return df, region_map, baseline_metrics, final_test, feat_imp


# ---------------------------------------------------------------------------
# 2.  Helper
# ---------------------------------------------------------------------------

def export(fig, name):
    for ext in [".pdf", ".png"]:
        fig.savefig(IMG_DIR / f"{name}{ext}", dpi=DPI, bbox_inches="tight", pad_inches=0.1)
    plt.close(fig)
    print(f"  ✓  {name}")


# ===================================================================
#  EDA  —  all plots take (df, region_map) as first arguments
# ===================================================================

# --- categorical_shares --------------------------------------------------

def plot_categorical_shares(df, *_, **__):
    import matplotlib.ticker as mticker
    from matplotlib.patches import Patch

    variables = [
        "category_sub", "ownership_type", "energy_class",
        "construction_type", "building_condition", "location_type",
    ]
    var_labels = {
        "category_sub": "Dispozice bytu",
        "ownership_type": "Typ vlastnictví",
        "energy_class": "Energetická třída",
        "construction_type": "Typ konstrukce",
        "building_condition": "Stav budovy",
        "location_type": "Typ lokality",
    }
    ordinal = {"building_condition", "energy_class"}
    MISSING_COLOR = "#b0b0b0"

    def _text_color(rgb):
        lum = 0.299 * rgb[0] + 0.587 * rgb[1] + 0.114 * rgb[2]
        return "black" if lum > 0.5 else "white"

    cat_order = {
        "category_sub": ["1+kk", "1+1", "2+kk", "2+1", "3+kk", "3+1",
                         "4+kk", "4+1", "5+", "atypicky"],
        "ownership_type": None,
        "energy_class": ["A", "B", "C", "D", "E", "F", "G"],
        "construction_type": None,
        "building_condition": ["Špatný", "Před rekonstrukcí", "Dobrý", "Velmi dobrý",
                               "Po rekonstrukci", "V rekonstrukci", "Novostavba",
                               "Ve výstavbě", "Projekt"],
        "location_type": None,
    }

    fig, axes = plt.subplots(6, 1, figsize=(FULL + 5, 18))
    for var, ax in zip(variables, axes):
        counts = df[var].value_counts(dropna=False, normalize=True) * 100
        order = cat_order.get(var)
        if order is not None:
            present = [c for c in order if c in counts.index]
            others = [c for c in counts.index if c not in order and c is not None and pd.notna(c)]
            miss = [c for c in counts.index if pd.isna(c)]
            counts = counts.reindex(present + others + miss).dropna()
        else:
            first = df[var].dropna().unique().tolist()
            miss = [c for c in counts.index if pd.isna(c)]
            counts = counts.reindex(first + miss).dropna()

        n_cats = counts.index.dropna().shape[0] if counts.index.isna().any() else len(counts)
        if var in ordinal:
            pal = sns.color_palette("RdYlGn_r", n_cats)
        else:
            pal = sns.color_palette("colorblind", n_cats)

        cumul = 0.0
        ci = 0
        legend_entries = []
        for cat, pct in counts.items():
            missing = pd.isna(cat)
            c = MISSING_COLOR if missing else pal[ci % len(pal)]
            ci += 0 if missing else 1
            ax.barh(0, pct, left=cumul, color=c, height=1.5,
                    edgecolor="white", linewidth=0.5)
            if pct >= 4:
                cx = cumul + pct / 2
                ax.text(cx, 0, f"{pct:.0f}%", ha="center", va="center",
                        fontsize=13, fontweight="bold",
                        color=_text_color(c if not missing else (0.5, 0.5, 0.5)))
            label = str(cat) if not missing else "(missing)"
            legend_entries.append(Patch(facecolor=c, label=f"{label}  ({pct:.1f}%)"))
            cumul += pct

        ax.set_xlim(0, 100)
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f} %"))
        ax.set_yticks([])
        ax.set_ylabel(var_labels.get(var, var), fontsize=14, labelpad=14)
        ax.set_xlabel("Podíl na trhu", fontsize=12)
        leg = ax.legend(handles=legend_entries, frameon=True, fontsize=13,
                        loc="center left", bbox_to_anchor=(1.02, 0.5),
                        handlelength=1.0, handleheight=1.0,
                        title=var_labels.get(var, var), title_fontsize=14)
        for t in leg.get_texts():
            t.set_va("center")
        sns.despine(ax=ax, left=True, bottom=False)
    plt.tight_layout()
    export(fig, "categorical_shares")

# --- price_hist ----------------------------------------------------------

def plot_price_hist(df, *_, **__):
    fig, ax = plt.subplots(figsize=(HALF, 2.5))
    price = df["price_total"] / 1e6
    sns.histplot(price, bins=20, kde=True, color=COLOR_PAL[0],
                 edgecolor="white", linewidth=0.3, ax=ax)
    sns.rugplot(price, height=0.05, color=COLOR_PAL[0], alpha=0.4, ax=ax)
    ax.set_xlabel("Cena (mil. Kč)")
    ax.set_ylabel("Počet")
    sns.despine()
    export(fig, "price_hist")


# --- area_price ----------------------------------------------------------

def plot_area_price(df, *_, **__):
    fig, axes = plt.subplots(1, 2, figsize=(FULL, 3), sharey=False)

    for ax, y_col, label in zip(
        axes, ["price_total", "price_total"],
        ["Cena (mil. Kč)", "Cena (Kč)"]
    ):
        y = df[y_col]
        x = df["usable_area_m2"]
        if ax == axes[1]:
            y = np.log1p(y)
            label = "log(Cena)"
        else:
            y = y / 1e6
        ci = COLOR_PAL[0] if ax == axes[0] else COLOR_PAL[2]
        sns.regplot(x=x, y=y, scatter_kws={"s": 6, "alpha": 0.3, "color": ci},
                    line_kws={"color": "crimson", "lw": 1.5}, ax=ax, ci=None)
        r, _ = stats.pearsonr(x, y)
        ax.text(0.05, 0.93, f"$R^2 = {r**2:.2f}$", transform=ax.transAxes,
                va="top", ha="left", fontsize=8,
                bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.7))
        ax.set_xlabel("Užitná plocha (m²)")
        ax.set_ylabel(label)
        sns.despine(ax=ax)
    plt.tight_layout(pad=0.8)
    export(fig, "area_price")


# --- missings ------------------------------------------------------------

def plot_missings(df, *_, **__):
    missing = df.isnull().mean() * 100
    missing = missing[missing > 0].sort_values(ascending=True)
    if missing.empty:
        return
    n = len(missing)
    fig, ax = plt.subplots(figsize=(HALF, max(2.5, n * 0.32)))
    ax.barh(range(n), missing.values, color=COLOR_PAL[0], height=0.7)
    ax.set_yticks(range(n))
    ax.set_yticklabels(missing.index, fontsize=8)
    ax.set_xlabel("Chybějící hodnoty (%)")
    ax.set_ylabel("")
    for i, v in enumerate(missing.values):
        ax.text(v + 0.8, i, f"{v:.1f}%", va="center", fontsize=8)
    ax.set_xlim(0, missing.max() + 12)
    sns.despine(left=True)
    export(fig, "missings")


# --- floor_price ----------------------------------------------------------

def plot_floor_price(df, *_, **__):
    d = df.dropna(subset=["floor_number", "price_total", "usable_area_m2"]).copy()
    d["price_m2"] = d["price_total"] / d["usable_area_m2"] / 1000
    cap = d["price_m2"].quantile(0.995)
    d = d[d["price_m2"] < cap]
    d["floor_number"] = d["floor_number"].astype(int)
    counts = d["floor_number"].value_counts()
    valid = counts[counts >= 10].index
    d = d[d["floor_number"].isin(valid)]
    order = sorted(d["floor_number"].unique())
    n = len(order)
    pal = sns.color_palette("viridis", n)
    fig, ax = plt.subplots(figsize=(FULL, 6.5))
    sns.violinplot(data=d, x="price_m2", y="floor_number", order=order,
                   inner="box", cut=0.3, palette=pal, ax=ax, orient="h")
    ax.invert_yaxis()
    ax.set_xlim(left=0)
    ax.xaxis.grid(True, linestyle="--", color="#b0b0b0", alpha=0.4)
    ax.set_axisbelow(True)
    ax.set_ylabel("Podlaží bytu", fontsize=14)
    ax.set_xlabel("Cena za m² (tis. Kč)", fontsize=13)
    ax.tick_params(axis="both", labelsize=12)
    sns.despine()
    export(fig, "floor_price")


# --- bar_counts ----------------------------------------------------------

def plot_bar_counts(df, *_, **__):
    counts = df["category_sub"].value_counts()
    n = len(counts)
    fig, ax = plt.subplots(figsize=(HALF, max(2.5, n * 0.35)))
    ax.barh(range(n), counts.values, color=COLOR_PAL[0], height=0.7)
    ax.set_yticks(range(n))
    ax.set_yticklabels(counts.index, fontsize=8)
    ax.set_xlabel("Počet nabídek")
    ax.set_ylabel("")
    for i, v in enumerate(counts.values):
        pct = v / counts.sum() * 100
        ax.text(v + 5, i, f"{v} ({pct:.1f}%)", va="center", fontsize=8)
    sns.despine(left=True)
    export(fig, "bar_counts")


# --- disp_o --------------------------------------------------------------

def plot_disp_o(df, *_, **__):
    counts = df["category_sub"].value_counts().sort_values(ascending=False)
    colors = [COLOR_PAL[0] if "kk" in c else COLOR_PAL[2] for c in counts.index]
    n = len(counts)
    fig, ax = plt.subplots(figsize=(HALF, max(2.5, n * 0.35)))
    ax.barh(range(n), counts.values, color=colors, height=0.7)
    ax.set_yticks(range(n))
    ax.set_yticklabels(counts.index, fontsize=8)
    ax.set_xlabel("Počet nabídek")
    ax.set_ylabel("")
    for i, v in enumerate(counts.values):
        pct = v / counts.sum() * 100
        ax.text(v + 5, i, f"{v} ({pct:.1f}%)", va="center", fontsize=8)
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLOR_PAL[0], label="kk (moderní)"),
        Patch(facecolor=COLOR_PAL[2], label="+1 (tradiční)")
    ]
    ax.legend(handles=legend_elements, frameon=False, fontsize=8)
    sns.despine(left=True)
    export(fig, "disp_o")


# --- room_price ----------------------------------------------------------

def plot_room_price(df, *_, **__):
    d = df.dropna(subset=["category_sub", "price_total", "usable_area_m2"]).copy()
    d["price_m2"] = d["price_total"] / d["usable_area_m2"] / 1000
    cap = d["price_m2"].quantile(0.995)
    d = d[d["price_m2"] < cap]
    d["rooms"] = d["category_sub"].str.extract(r"(\d+)").astype(float)
    d = d.dropna(subset=["rooms"])
    d["rooms"] = d["rooms"].astype(int)
    counts = d["rooms"].value_counts()
    valid = counts[counts >= 10].index
    d = d[d["rooms"].isin(valid)]
    order = sorted(d["rooms"].unique())
    n = len(order)
    fig, ax = plt.subplots(figsize=(FULL, max(2.5, n * 0.35)))
    sns.violinplot(data=d, x="price_m2", y="rooms", order=order,
                   inner="box", cut=0, color=COLOR_PAL[2], ax=ax, orient="h")
    ax.set_ylabel("Počet místností")
    ax.set_xlabel("Cena za m² (tis. Kč)")
    sns.despine()
    export(fig, "room_price")


# --- price_cond ----------------------------------------------------------

def plot_price_cond(df, *_, **__):
    d = df.dropna(subset=["building_condition", "price_total", "usable_area_m2"]).copy()
    d = d[d["building_condition"] != "Špatný"]
    d["price_m2"] = d["price_total"] / d["usable_area_m2"] / 1000
    cap = d["price_m2"].quantile(0.995)
    d = d[d["price_m2"] < cap]
    order = ["Před rekonstrukcí", "Dobrý", "Velmi dobrý",
             "Po rekonstrukci", "V rekonstrukci", "Novostavba",
             "Ve výstavbě", "Projekt"]
    order = [c for c in order if c in d["building_condition"].values]
    n = len(order)
    pal = sns.color_palette("crest", n)
    fig, ax = plt.subplots(figsize=(FULL, 5.5))
    sns.violinplot(data=d, x="price_m2", y="building_condition", order=order,
                   inner="quartile", cut=0, palette=pal, ax=ax, orient="h")
    ax.set_xlim(left=0)
    ax.xaxis.grid(True, linestyle="--", color="#b0b0b0", alpha=0.4)
    ax.set_axisbelow(True)
    ax.set_ylabel("Stav budovy", fontsize=14)
    ax.set_xlabel("Cena za m² (tis. Kč)", fontsize=13)
    ax.tick_params(axis="both", labelsize=12)
    if ax.get_legend():
        ax.get_legend().remove()
    sns.despine()
    export(fig, "price_cond")


# --- construct_price ------------------------------------------------------

def plot_construct_price(df, *_, **__):
    d = df.dropna(subset=["construction_type", "price_total", "usable_area_m2"]).copy()
    d["price_m2"] = d["price_total"] / d["usable_area_m2"] / 1000
    cap = d["price_m2"].quantile(0.995)
    d = d[d["price_m2"] < cap]
    order = d.groupby("construction_type")["price_m2"].median().sort_values().index.tolist()
    n = len(order)
    fig, ax = plt.subplots(figsize=(FULL, max(2.5, n * 0.38)))
    sns.violinplot(data=d, x="price_m2", y="construction_type", order=order,
                   inner="box", cut=0, color=COLOR_PAL[4], ax=ax, orient="h")
    ax.set_ylabel("Typ konstrukce")
    ax.set_xlabel("Cena za m² (tis. Kč)")
    sns.despine()
    export(fig, "construct_price")


# --- region_price_violin --------------------------------------------------

def plot_region_price_violin(df, region_map, *_, **__):
    d = df.dropna(subset=["locality_region_id", "price_total", "usable_area_m2"]).copy()
    d["price_m2"] = d["price_total"] / d["usable_area_m2"] / 1000
    cap = d["price_m2"].quantile(0.995)
    d = d[d["price_m2"] < cap]
    d = d.merge(region_map, on="locality_region_id", how="left")
    order = d.groupby("name")["price_m2"].median().sort_values().index.tolist()
    n = len(order)
    colors_seq = sns.color_palette("viridis_r", n)
    fig, ax = plt.subplots(figsize=(FULL, 7))
    sns.violinplot(data=d, x="price_m2", y="name", order=order,
                   palette=colors_seq, inner="quartile",
                   cut=0, ax=ax, orient="h")
    ax.set_ylabel("Kraj", fontsize=14)
    ax.set_xlabel("Cena za m² (tis. Kč)", fontsize=13)
    ax.tick_params(labelsize=12)
    ax.tick_params(axis="y", length=0)
    ax.set_xlim(left=0)
    ax.xaxis.grid(True, linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)
    sns.despine()
    export(fig, "region_price_violin")


# ===================================================================
#  Model comparison  —  plots from CSV artifacts
# ===================================================================

# --- linmodels -----------------------------------------------------------

def plot_linmodels(_, __, baseline_metrics, ___, ____):
    df = baseline_metrics[baseline_metrics["model"].str.contains("Linear|Hierarchical")].copy()
    df = df.sort_values("MedAPE", ascending=True)
    labels = [
        "Ridge" if "Ridge" in m else "Linear" if "Linear" in m else "Median"
        for m in df["model"]
    ]
    n = len(df)
    fig, axes = plt.subplots(1, 2, figsize=(FULL, 2.8), sharey=False)
    metrics = ["MedAPE", "R2"]
    titles = ["MedAPE", "$R^2$"]
    for ax, met, t in zip(axes, metrics, titles):
        vals = df[met].values
        if met == "MedAPE":
            vals = vals * 100
            t = "MedAPE (%)"
        colors_ = [COLOR_PAL[0]] * n if met == "R2" else [COLOR_PAL[2]] * n
        ax.barh(range(n), vals, color=colors_, height=0.6)
        ax.set_yticks(range(n))
        ax.set_yticklabels(labels, fontsize=8)
        ax.set_xlabel(t)
        ax.invert_yaxis()
        sns.despine(ax=ax, left=True)
    plt.tight_layout(pad=0.8)
    export(fig, "linmodels")


# --- coefs ---------------------------------------------------------------

def plot_coefs(df, region_map, baseline_metrics, final_test, feat_imp):
    """Ridge regression coefficients — use feature importances as a proxy."""
    top = feat_imp.head(20).sort_values("importance", ascending=False)
    most_important = top["importance"].max()
    colors_ = [COLOR_PAL[0] if v > 0 else COLOR_PAL[2] for v in top["importance"]]
    n = len(top)
    fig, ax = plt.subplots(figsize=(HALF, max(3, n * 0.32)))
    ax.barh(range(n), top["importance"], color=colors_, height=0.7)
    ax.set_yticks(range(n))
    labels = [col.replace("ordinal__", "").replace("log_num__", "")
                   .replace("cat__", "").replace("num__", "")
                   .replace("bool__", "").replace("te_", "")
                   .replace("structural__", "")
              for col in top["feature"]]
    ax.set_yticklabels(labels, fontsize=7.5)
    ax.set_xlabel("Důležitost (gain)")
    sns.despine(left=True)
    export(fig, "coefs")


# --- gain ----------------------------------------------------------------

def plot_gain(_, __, ___, ____, feat_imp):
    top = feat_imp.head(20).sort_values("importance", ascending=False)
    n = len(top)
    fig, ax = plt.subplots(figsize=(HALF, max(3, n * 0.32)))
    ax.barh(range(n), top["importance"], color=COLOR_PAL[3], height=0.7)
    ax.set_yticks(range(n))
    labels = [col.replace("ordinal__", "").replace("log_num__", "")
                   .replace("cat__", "").replace("num__", "")
                   .replace("bool__", "").replace("te_", "")
                   .replace("structural__", "")
              for col in top["feature"]]
    ax.set_yticklabels(labels, fontsize=7.5)
    ax.set_xlabel("Důležitost (gain)")
    sns.despine(left=True)
    export(fig, "gain")


# --- baseline ------------------------------------------------------------

def plot_baseline(_, __, baseline_metrics, ___, ____):
    df = baseline_metrics.sort_values("MedAPE", ascending=True)
    short_names = {
        "XGBoost baseline": "XGBoost",
        "Linear regression on log-price": "Lineární",
        "Hierarchical median baseline": "Medián",
    }
    labels = [short_names.get(m, m) for m in df["model"]]
    n = len(df)
    fig, axes = plt.subplots(1, 2, figsize=(FULL, 2.5), sharey=False)
    for ax, met, t in zip(axes, ["MedAPE", "R2"], ["MedAPE", "$R^2$"]):
        vals = df[met].values
        if met == "MedAPE":
            vals = vals * 100
            t = "MedAPE (%)"
        ci = COLOR_PAL[2] if met == "MedAPE" else COLOR_PAL[0]
        ax.barh(range(n), vals, color=ci, height=0.6)
        ax.set_yticks(range(n))
        ax.set_yticklabels(labels, fontsize=8)
        ax.set_xlabel(t)
        ax.invert_yaxis()
        sns.despine(ax=ax, left=True)
    plt.tight_layout(pad=0.8)
    export(fig, "baseline")


# ===================================================================
#  SHAP  —  requires trained model
# ===================================================================


def _load_model_and_data():
    import sys
    sys.path.insert(0, str(REPO_ROOT))
    import joblib
    try:
        from src.pipe import Model_pipeline  # noqa
    except ImportError:
        pass
    m = joblib.load(MODELS_DIR / "best_xgb_pipeline.joblib")
    df = pd.read_csv(DATA_DIR / "apartments_raw_data.csv")
    return m, df


def _get_xgb_booster(model):
    reg = model.regressor_ if hasattr(model, "regressor_") else model
    xgb = reg.pipeline_.named_steps["model"]
    return xgb


def _get_preprocessor(model):
    reg = model.regressor_ if hasattr(model, "regressor_") else model
    pipe = reg.pipeline_
    for key in ["preprocessing", "preprocessor"]:
        if key in pipe.named_steps:
            return pipe.named_steps[key]
    raise KeyError(f"no preprocessor step in {list(pipe.named_steps.keys())}")


def _prepare_shap_data(model, df, n=500):
    """Select and prepare the columns the model expects."""
    reg = model.regressor_ if hasattr(model, "regressor_") else model
    cfg = reg.config
    needed = set()
    for k in ["num_features", "log_num_features", "bool_features",
              "cat_features", "structural_features", "target_encoded_features",
              "ordinal_features"]:
        needed.update(cfg.get(k, []))
    needed.add("price_total")
    cols = [c for c in needed if c in df.columns]
    sample = df[cols].dropna().head(n).copy()
    if "total_area_m2_was_missing" in cfg.get("bool_features", []) and "total_area_m2_was_missing" not in sample.columns:
        if "total_area_m2" in sample.columns:
            sample["total_area_m2_was_missing"] = sample["total_area_m2"].isna().astype(int)
        else:
            sample["total_area_m2_was_missing"] = 0
    return sample


def _shap_transform_and_explain(model, X_df):
    """Transform DataFrame and compute SHAP values. Returns (X_trans DataFrame, shap_values, explainer, short_names)."""
    import shap
    preproc = _get_preprocessor(model)
    xgb = _get_xgb_booster(model)
    X_trans = preproc.transform(X_df)
    feature_names = preproc.get_feature_names_out()
    short_names = [n.replace("ordinal__", "").replace("log_num__", "")
                    .replace("cat__", "").replace("num__", "")
                    .replace("bool__", "").replace("te_", "")
                    .replace("structural__", "") for n in feature_names]
    explainer = shap.TreeExplainer(xgb)
    shap_values = explainer.shap_values(X_trans)
    return X_trans, shap_values, explainer, short_names


def _shap_data_for_single(df_orig, model, usable_quantile):
    """Get a single-row DataFrame at the given usable_area_m2 quantile."""
    q = df_orig["usable_area_m2"].quantile(usable_quantile)
    idx = (df_orig["usable_area_m2"] - q).abs().idxmin()
    row = df_orig.loc[[idx]]
    reg = model.regressor_ if hasattr(model, "regressor_") else model
    cfg = reg.config
    needed = set()
    for k in ["num_features", "log_num_features", "bool_features",
              "cat_features", "structural_features", "target_encoded_features",
              "ordinal_features"]:
        needed.update(cfg.get(k, []))
    needed.add("price_total")
    cols = [c for c in needed if c in row.columns]
    # fill NaN row-wide so the single row survives preprocessing
    s = row[cols].copy()
    for c in s.columns:
        if s[c].isna().any():
            if s[c].dtype == object:
                s[c] = s[c].fillna("missing")
            else:
                s[c] = s[c].fillna(0)
    if "total_area_m2_was_missing" not in s.columns:
        if "total_area_m2" in s.columns:
            s["total_area_m2_was_missing"] = s["total_area_m2"].isna().astype(int)
        else:
            s["total_area_m2_was_missing"] = 0
    return s


# --- shap4 (beeswarm) ---------------------------------------------------

def plot_shap4(_, __, ___, ____, feat_imp):
    try:
        import shap
        model, df = _load_model_and_data()
        X_df = _prepare_shap_data(model, df, n=500)
        _ = X_df.pop("price_total")
        X_trans, shap_values, _, short_names = _shap_transform_and_explain(model, X_df)
        X_arr = np.asarray(X_trans)
        fig, ax = plt.subplots(figsize=(FULL, 4.5))
        shap.summary_plot(shap_values, X_arr, feature_names=short_names,
                          max_display=15, show=False, color_bar=True)
        ax = plt.gca()
        ax.set_xlabel("SHAP hodnota (vliv na výstup)")
        sns.despine()
        export(fig, "shap4")
    except Exception as e:
        import traceback
        print(f"  ⚠  shap4 skipped: {e}")
        traceback.print_exc()


# --- shap3 (dependence) --------------------------------------------------

def plot_shap3(_, __, ___, ____, feat_imp):
    try:
        import shap
        model, df = _load_model_and_data()
        X_df = _prepare_shap_data(model, df, n=500)
        _ = X_df.pop("price_total")
        X_trans, shap_values, _, short_names = _shap_transform_and_explain(model, X_df)
        X_arr = np.asarray(X_trans)
        feature_names = _get_preprocessor(model).get_feature_names_out()
        floor_idx = next((i for i, n in enumerate(feature_names) if "floor_number" in n), None)
        if floor_idx is None:
            raise ValueError("floor_number not found")
        fig, ax = plt.subplots(figsize=(HALF, 3))
        shap.dependence_plot(floor_idx, shap_values, X_arr,
                             feature_names=short_names, ax=ax, show=False)
        ax.set_xlabel("Číslo podlaží")
        ax.set_ylabel("SHAP hodnota")
        sns.despine()
        export(fig, "shap3")
    except Exception as e:
        import traceback
        print(f"  ⚠  shap3 skipped: {e}")
        traceback.print_exc()


# --- shap1 + shap2 (waterfall) ------------------------------------------

def _plot_shap_waterfall(df_orig, model, quantile, title, filename):
    import shap
    X_df = _shap_data_for_single(df_orig, model, quantile)
    _ = X_df.pop("price_total")
    X_trans, shap_values, explainer, short_names = _shap_transform_and_explain(model, X_df)
    X_arr = np.asarray(X_trans)

    shap.waterfall_plot(
        shap.Explanation(values=shap_values[0],
                         base_values=float(explainer.expected_value),
                         data=X_arr[0],
                         feature_names=short_names),
        max_display=12, show=False
    )
    fig = plt.gcf()
    fig.set_size_inches(HALF, 3.5)
    sns.despine()
    export(fig, filename)


def plot_shap1(_, __, ___, ____, feat_imp):
    try:
        model, df = _load_model_and_data()
        _plot_shap_waterfall(df, model, 0.25, "Malý byt (Příklad predikce)", "shap1")
    except Exception as e:
        import traceback
        print(f"  ⚠  shap1 skipped: {e}")
        traceback.print_exc()


def plot_shap2(_, __, ___, ____, feat_imp):
    try:
        model, df = _load_model_and_data()
        _plot_shap_waterfall(df, model, 0.75, "Velký byt (Příklad predikce)", "shap2")
    except Exception as e:
        import traceback
        print(f"  ⚠  shap2 skipped: {e}")
        traceback.print_exc()


# --- shap_poi helpers ----------------------------------------------------

EARTH_RADIUS_KM = 6371.0088
TOP5_POI = [
    "transport_quality_score", "city_center_travel_time_min",
    "convenience_nearest_km", "grocery_avg_3_nearest_km",
    "supermarket_nearest_km",
]
CITY_TRAVEL_PATH = REPO_ROOT / "data" / "city_center_travel_times.csv"


def _build_transport_quality_score(apt_lat, apt_lon, transport_df,
                                    w_metro=10.0, w_tram=1.0, w_bus=0.0):
    scores = np.zeros(len(apt_lat))
    apt_rad = np.radians(np.column_stack([apt_lat, apt_lon]))
    for kind, weight in [("metro", w_metro), ("tram_stop", w_tram), ("bus_stop", w_bus)]:
        if weight == 0:
            continue
        mask = transport_df["poi_kind"].str.lower() == kind
        if not mask.any():
            continue
        poi_rad = np.radians(
            transport_df.loc[mask, ["latitude", "longitude"]].astype(float).values
        )
        tree = BallTree(poi_rad, metric="haversine")
        dist_km, _ = tree.query(apt_rad, k=1)
        dist_km = dist_km.flatten() * EARTH_RADIUS_KM
        scores += weight / (1.0 + dist_km)
    return scores


def _add_poi_features(df, transport_df, grocery_df):
    df = df.copy()
    apt_lat = df["latitude"].astype(float).values
    apt_lon = df["longitude"].astype(float).values
    apt_rad = np.radians(np.column_stack([apt_lat, apt_lon]))

    df["transport_quality_score"] = _build_transport_quality_score(
        apt_lat, apt_lon, transport_df
    )

    sup_mask = grocery_df["poi_kind"].str.lower() == "supermarket"
    if sup_mask.any():
        sup_rad = np.radians(
            grocery_df.loc[sup_mask, ["latitude", "longitude"]].astype(float).values
        )
        sup_tree = BallTree(sup_rad, metric="haversine")
        df["supermarket_nearest_km"] = (
            sup_tree.query(apt_rad, k=1)[0].flatten() * EARTH_RADIUS_KM
        )

    con_mask = ~sup_mask
    if con_mask.any():
        con_rad = np.radians(
            grocery_df.loc[con_mask, ["latitude", "longitude"]].astype(float).values
        )
        con_tree = BallTree(con_rad, metric="haversine")
        df["convenience_nearest_km"] = (
            con_tree.query(apt_rad, k=1)[0].flatten() * EARTH_RADIUS_KM
        )

    grocery_rad = np.radians(
        grocery_df[["latitude", "longitude"]].astype(float).values
    )
    grocery_tree = BallTree(grocery_rad, metric="haversine")
    dist_3, _ = grocery_tree.query(apt_rad, k=3)
    df["grocery_avg_3_nearest_km"] = np.mean(dist_3, axis=1) * EARTH_RADIUS_KM

    travel = pd.read_csv(CITY_TRAVEL_PATH)
    travel_map = dict(zip(travel["listing_id"], travel["travel_time_min"]))
    df["city_center_travel_time_min"] = df["id"].map(travel_map).fillna(0.0)

    return df


# --- shap_poi (beeswarm for POI model) -----------------------------------

def plot_shap_poi(_, __, ___, ____, feat_imp):
    try:
        import shap
        import joblib
        import sys
        sys.path.insert(0, str(REPO_ROOT))
        from src.poi_models_workflow import load_experiment_data
        from src.process import process_df

        model = joblib.load(MODELS_DIR / "tuned_xgb_top5_poi.joblib")
        data = load_experiment_data()
        df = _add_poi_features(data.train_fe, data.transport_poi, data.grocery_poi)
        df = process_df(df)

        X_df = _prepare_shap_data(model, df, n=500)
        _ = X_df.pop("price_total")
        X_trans, shap_values, _, short_names = _shap_transform_and_explain(model, X_df)
        X_arr = np.asarray(X_trans)
        fig, ax = plt.subplots(figsize=(FULL, 4.5))
        shap.summary_plot(shap_values, X_arr, feature_names=short_names,
                          max_display=15, show=False, color_bar=True)
        ax = plt.gca()
        ax.set_xlabel("SHAP hodnota (vliv na výstup)")
        sns.despine()
        export(fig, "shap_poi")
    except Exception as e:
        import traceback
        print(f"  ⚠  shap_poi skipped: {e}")
        traceback.print_exc()


# ===================================================================
#  Main
# ===================================================================

def main():
    print("Loading data …")
    data = load_data()
    print(f"  {len(data[0])} rows in main dataset")
    print(f"  {len(data[1])} regions")

    plots = [
        ("categorical_shares",   plot_categorical_shares),
        ("price_hist",           plot_price_hist),
        ("area_price",           plot_area_price),
        ("missings",             plot_missings),
        ("floor_price",          plot_floor_price),
        ("bar_counts",           plot_bar_counts),
        ("disp_o",               plot_disp_o),
        ("room_price",           plot_room_price),
        ("price_cond",           plot_price_cond),
        ("construct_price",      plot_construct_price),
        ("region_price_violin",  plot_region_price_violin),
        ("linmodels",            plot_linmodels),
        ("coefs",                plot_coefs),
        ("gain",                 plot_gain),
        ("baseline",             plot_baseline),
        ("shap1",                plot_shap1),
        ("shap2",                plot_shap2),
        ("shap3",                plot_shap3),
        ("shap4",                plot_shap4),
        ("shap_poi",             plot_shap_poi),
    ]

    for name, func in plots:
        print(f"\n── {name} ", end="")
        try:
            func(*data)
        except Exception as e:
            print(f"  ✗  {name} failed: {e}")

    print(f"\nDone — {len(list(IMG_DIR.glob('*.png')))} images in {IMG_DIR}")


if __name__ == "__main__":
    main()
