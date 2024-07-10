from typing import Optional
import os
import glob
import shutil
from driftpy.drift_client import DriftClient
from driftpy.pickle.vat import Vat

from driftpy.user_map.user_map_config import (
    UserMapConfig,
    UserStatsMapConfig,
    WebsocketConfig as UserMapWebsocketConfig,
)
from driftpy.user_map.user_map import UserMap
from driftpy.user_map.userstats_map import UserStatsMap

from driftpy.market_map.market_map_config import (
    MarketMapConfig,
    WebsocketConfig as MarketMapWebsocketConfig,
)
from driftpy.market_map.market_map import MarketMap
from driftpy.types import MarketType, OraclePriceData
from driftpy.constants.perp_markets import mainnet_perp_market_configs
from driftpy.constants.spot_markets import mainnet_spot_market_configs
from driftpy.constants.numeric_constants import *
from driftpy.math.margin import MarginCategory
from driftpy.drift_user import DriftUser
import copy


def sort_pickles(source_dir: str, log):
    pickles_folder = os.path.join(source_dir, "pickles")
    os.makedirs(pickles_folder, exist_ok=True)

    pkl_files = glob.glob(os.path.join(source_dir, "*.pkl"))

    for file in pkl_files:
        filename = os.path.basename(file)
        dest = os.path.join(pickles_folder, filename)

        try:
            shutil.move(file, dest)
        except shutil.Error as e:
            log.info(e)
        except Exception as e:
            log.info(e)


def load_newest_files(directory: Optional[str] = None) -> dict[str, str]:
    directory = directory or os.getcwd()

    newest_files: dict[str, tuple[str, int]] = {}

    prefixes = ["perp", "perporacles", "spot", "spotoracles", "usermap", "userstats"]

    for filename in os.listdir(directory):
        if filename.endswith(".pkl") and any(
            filename.startswith(prefix + "_") for prefix in prefixes
        ):
            start = filename.index("_") + 1
            prefix = filename[: start - 1]
            end = filename.index(".")
            slot = int(filename[start:end])
            if not prefix in newest_files or slot > newest_files[prefix][1]:
                newest_files[prefix] = (directory + "/" + filename, slot)

    # mapping e.g { 'spotoracles' : 'spotoracles_272636137.pkl' }
    prefix_to_filename = {
        prefix: filename for prefix, (filename, _) in newest_files.items()
    }

    return prefix_to_filename


async def load_vat(
    dc: DriftClient, pickle_map: dict[str, str], env: str = "prod"
) -> Vat:
    user, stats, spot, perp = get_maps(dc)

    user_filename = pickle_map["usermap"]
    stats_filename = pickle_map["userstats"]
    perp_filename = pickle_map["perp"]
    spot_filename = pickle_map["spot"]
    perp_oracles_filename = pickle_map["perporacles"]
    spot_oracles_filename = pickle_map["spotoracles"]

    vat = Vat(dc, user, stats, spot, perp)

    await vat.unpickle(
        user_filename,
        stats_filename,
        spot_filename,
        perp_filename,
        spot_oracles_filename,
        perp_oracles_filename,
    )

    if env == "dev":
        users = []
        for user in vat.users.values():
            value = user.get_net_spot_market_value(None) + user.get_unrealized_pnl(True)
            users.append(
                (value, user.user_public_key, user.get_user_account_and_slot())
            )
        users.sort(key=lambda x: x[0], reverse=True)
        vat.users.clear()
        for user in users[:100]:
            await vat.users.add_pubkey(user[1], user[2])

    for user in vat.users.values():
        user.drift_client = dc

    return vat


def get_maps(dc: DriftClient):
    perp = MarketMap(
        MarketMapConfig(
            dc.program, MarketType.Perp(), MarketMapWebsocketConfig(), dc.connection
        )
    )

    spot = MarketMap(
        MarketMapConfig(
            dc.program, MarketType.Spot(), MarketMapWebsocketConfig(), dc.connection
        )
    )

    user = UserMap(UserMapConfig(dc, UserMapWebsocketConfig()))

    stats = UserStatsMap(UserStatsMapConfig(dc))

    return user, stats, spot, perp
