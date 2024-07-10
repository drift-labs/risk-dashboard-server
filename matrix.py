import pandas as pd  # type: ignore
from driftpy.drift_client import DriftClient
from driftpy.pickle.vat import Vat
from driftpy.constants.spot_markets import mainnet_spot_market_configs

NUMBER_OF_SPOT = len(mainnet_spot_market_configs)


def get_matrix(
    levs_none,
    levs_init,
    levs_maint,
    user_keys,
    env="mainnet",
    mode=0,
    perp_market_inspect=0,
    health=50,
):
    if mode == 2:
        levs_maint = [x for x in levs_maint if int(x["health"]) <= int(health)]
    if mode == 3:
        levs_init = [x for x in levs_init if int(x["health"]) <= int(health)]

    df: pd.DataFrame
    match mode:
        case 0:  # nothing
            df = pd.DataFrame(levs_none, index=user_keys)
        case 1:  # liq within 50% of oracle
            df = pd.DataFrame(levs_none, index=user_keys)
        case 2:  # maint. health < 10%
            user_keys = [x["user_key"] for x in levs_init]
            df = pd.DataFrame(levs_init, index=user_keys)
        case 3:  # init. health < 10%
            user_keys = [x["user_key"] for x in levs_maint]
            df = pd.DataFrame(levs_maint, index=user_keys)

    def get_rattt(row):
        calculations = [
            (
                "all_assets",
                lambda v: v if v > 0 else 0,
            ),
            (
                "all",
                lambda v: v
                / row["spot_asset"]
                * (row["perp_liability"] + row["spot_liability"])
                if v > 0
                else 0,
            ),
            ("leverage", lambda v: row["leverage"]),
            (
                "all_perp",
                lambda v: v / row["spot_asset"] * row["perp_liability"] if v > 0 else 0,
            ),
            (
                "all_spot",
                lambda v: v / row["spot_asset"] * row["spot_liability"] if v > 0 else 0,
            ),
            (
                f"perp_{perp_market_inspect}_long",
                lambda v: v / row["spot_asset"] * row["net_p"][perp_market_inspect]
                if v > 0 and row["net_p"][0] > 0
                else 0,
            ),
            (
                f"perp_{perp_market_inspect}_short",
                lambda v: v / row["spot_asset"] * row["net_p"][perp_market_inspect]
                if v > 0 and row["net_p"][perp_market_inspect] < 0
                else 0,
            ),
        ]

        series_list = []
        for suffix, calc_func in calculations:
            series = pd.Series([calc_func(val) for key, val in row["net_v"].items()])
            series.index = [f"spot_{x}_{suffix}" for x in series.index]
            series_list.append(series)

        return pd.concat(series_list)

    df = pd.concat([df, df.apply(get_rattt, axis=1)], axis=1)

    def calculate_effective_leverage(group):
        assets = group["all_assets"]
        liabilities = group["all_liabilities"]
        return liabilities / assets if assets != 0 else 0

    def format_with_checkmark(value, condition, mode, financial=False):
        if financial:
            formatted_value = f"${value:,.2f}"
        else:
            formatted_value = f"{value:.2f}"

        if condition and mode > 0:
            return f"{formatted_value} ✅"
        return formatted_value

    res = pd.DataFrame(
        {
            ("spot" + str(i)): (
                df[f"spot_{i}_all_assets"].sum(),
                format_with_checkmark(
                    df[f"spot_{i}_all"].sum(),
                    0 < df[f"spot_{i}_all"].sum() < 1_000_000,
                    mode,
                    financial=True,
                ),
                format_with_checkmark(
                    calculate_effective_leverage(
                        {
                            "all_assets": df[f"spot_{i}_all_assets"].sum(),
                            "all_liabilities": df[f"spot_{i}_all"].sum(),
                        }
                    ),
                    0
                    < calculate_effective_leverage(
                        {
                            "all_assets": df[f"spot_{i}_all_assets"].sum(),
                            "all_liabilities": df[f"spot_{i}_all"].sum(),
                        }
                    )
                    < 2,
                    mode,
                ),
                df[f"spot_{i}_all_spot"].sum(),
                df[f"spot_{i}_all_perp"].sum(),
                df[f"spot_{i}_perp_{perp_market_inspect}_long"].sum(),
                df[f"spot_{i}_perp_{perp_market_inspect}_short"].sum(),
            )
            for i in range(NUMBER_OF_SPOT)
        },
        index=[
            "all_assets",
            "all_liabilities",
            "effective_leverage",
            "all_spot",
            "all_perp",
            f"perp_{perp_market_inspect}_long",
            f"perp_{perp_market_inspect}_short",
        ],
    ).T

    res["all_liabilities"] = res["all_liabilities"].astype(str)
    res["effective_leverage"] = res["effective_leverage"].astype(str)

    if env == "mainnet":  # mainnet_spot_market_configs
        res.index = [x.symbol for x in mainnet_spot_market_configs]
        res.index.name = "spot assets"  # type: ignore

    return res, df
