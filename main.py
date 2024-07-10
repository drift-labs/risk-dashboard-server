import os
import logging
import msgpack  # type: ignore

from dotenv import load_dotenv

from quart import Quart, Response

from driftpy.drift_client import DriftClient
from driftpy.pickle.vat import Vat
from driftpy.account_subscription_config import AccountSubscriptionConfig

from solana.rpc.async_api import AsyncClient

from anchorpy import Wallet

from utils import load_newest_files, get_maps, load_vat, sort_pickles
from matrix import get_matrix
from usermap import get_usermap_df

from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore
from apscheduler.triggers.interval import IntervalTrigger  # type: ignore

from hypercorn.config import Config
from hypercorn.asyncio import serve

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Quart(__name__)

load_dotenv()
url = os.getenv("RPC_URL")

connection = AsyncClient(url)

dc = DriftClient(
    connection,
    Wallet.dummy(),
    "mainnet",
    account_subscription=AccountSubscriptionConfig("cached"),
)

user, stats, spot, perp = get_maps(dc)
vat = Vat(dc, user, stats, spot, perp)

latest_prod_context = {
    "vat": None,
    "levs_none": None,
    "levs_init": None,
    "levs_maint": None,
    "user_keys": None,
    "res": None,
    "df": None,
}


latest_dev_context = {
    "vat": None,
    "levs_none": None,
    "levs_init": None,
    "levs_maint": None,
    "user_keys": None,
    "res": None,
    "df": None,
}


async def load_latest_contexts():
    logger.info("loading latest contexts")
    await dc.account_subscriber.update_cache()
    import time

    vat_start = time.time()
    try:
        await vat.pickle()
    except Exception as e:
        await vat.pickle()
    logger.info(f"loaded vat in {time.time() - vat_start} seconds")
    sort_start = time.time()
    sort_pickles(os.getcwd(), logger)
    logger.info(f"sorted pickles in {time.time() - sort_start} seconds")
    prod_start = time.time()
    await load_prod_context()
    logger.info(f"loaded prod context in {time.time() - prod_start} seconds")
    dev_start = time.time()
    await load_dev_context()
    logger.info(f"loaded dev context in {time.time() - dev_start} seconds")


async def load_prod_context():
    global latest_prod_context

    newest_snapshot = load_newest_files(os.getcwd() + "/pickles")
    vat = await load_vat(dc, newest_snapshot)

    (levs_none, levs_init, levs_maint), user_keys = await get_usermap_df(
        dc,
        vat.users,
        "margins",
        0,
        None,
        "ignore stables",
        n_scenarios=0,
        all_fields=True,
    )

    res, df = get_matrix(levs_none, levs_init, levs_maint, user_keys)

    latest_prod_context["vat"] = vat
    latest_prod_context["levs_none"] = levs_none
    latest_prod_context["levs_init"] = levs_init
    latest_prod_context["levs_maint"] = levs_maint
    latest_prod_context["user_keys"] = user_keys
    latest_prod_context["res"] = res
    latest_prod_context["df"] = df


async def load_dev_context():
    global latest_dev_context

    newest_snapshot = load_newest_files(os.getcwd() + "/pickles")
    vat = await load_vat(dc, newest_snapshot, "dev")

    (levs_none, levs_init, levs_maint), user_keys = await get_usermap_df(
        dc,
        vat.users,
        "margins",
        0,
        None,
        "ignore stables",
        n_scenarios=0,
        all_fields=True,
    )

    res, df = get_matrix(levs_none, levs_init, levs_maint, user_keys)

    latest_dev_context["vat"] = vat
    latest_dev_context["levs_none"] = levs_none
    latest_dev_context["levs_init"] = levs_init
    latest_dev_context["levs_maint"] = levs_maint
    latest_dev_context["user_keys"] = user_keys
    latest_dev_context["res"] = res
    latest_dev_context["df"] = df


scheduler = AsyncIOScheduler()
scheduler.add_job(
    func=load_latest_contexts,
    trigger=IntervalTrigger(hours=24),
    id="pickle",
    name="pickle",
    replace_existing=True,
)


@app.before_serving
async def start():
    logger.info("starting scheduler")
    await dc.subscribe()
    await load_latest_contexts()  # init load on startup
    scheduler.start()


@app.after_serving
async def shutdown():
    await dc.unsubscribe()
    scheduler.shutdown()


@app.route("/prod_context")
async def get_prod_context():
    data = {
        "levs_none": latest_prod_context["levs_none"],
        "levs_init": latest_prod_context["levs_init"],
        "levs_maint": latest_prod_context["levs_maint"],
        "user_keys": latest_prod_context["user_keys"],
        "res": latest_prod_context["res"].to_dict(),
        "df": latest_prod_context["df"].to_dict(),
    }

    response = Response(msgpack.packb(data), mimetype="application/msgpack")

    return response


@app.route("/dev_context")
async def get_dev_context():
    data = {
        "levs_none": latest_dev_context["levs_none"],
        "levs_init": latest_dev_context["levs_init"],
        "levs_maint": latest_dev_context["levs_maint"],
        "user_keys": latest_dev_context["user_keys"],
        "res": latest_dev_context["res"].to_dict(),
        "df": latest_dev_context["df"].to_dict(),
    }

    response = Response(msgpack.packb(data), mimetype="application/msgpack")

    return response


if __name__ == "__main__":
    import asyncio

    logger.info("starting server")
    config = Config()
    config.startup_timeout = 600
    asyncio.run(serve(app, config))
