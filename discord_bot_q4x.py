# 導入Discord.py模組
import discord
# 導入commands指令模組
from discord.ext import commands
from dotenv import load_dotenv
import functools
import os
import asyncio
from quantxlab.task import stock_list_quantx_ds, net_worth_ds, net_worth_linenotify, crawl_data

dbupdate_in_progress = False
qs_in_progress = False
qsboyd_in_progress = False

# intents是要求機器人的權限
intents = discord.Intents.all()
# command_prefix是前綴符號，可以自由選擇($, #, &...)
bot = commands.Bot(command_prefix="%", intents=intents)

@bot.event
# 當機器人完成啟動
async def on_ready():
    print(f"目前登入身份 --> {bot.user}")

@bot.command()
# 輸入%Hello呼叫指令
async def You_are_ugly(ctx):
    # 回覆Hello, world!
    await ctx.send("Yes, I am.")

@bot.command()
# 輸入%Hello呼叫指令
async def Hello(ctx):
    # 回覆Hello, world!
    await ctx.send("Hello, world!")

@bot.command()
async def Fuck(ctx):
    # 回覆Hello, world!
    await ctx.send("Fuck, world!")

@bot.command()
async def Tony(ctx):
    # 回覆Hello, world!
    await ctx.send("What a Handsome boy!")

@bot.command()
async def Jason(ctx):
    # 回覆Hello, world!
    await ctx.send("Fucking Handsome!")

@bot.command()
async def Boyd(ctx):
    # 回覆Hello, world!
    await ctx.send("風雲起　山河動　黃埔建軍聲勢雄\n革命壯士矢盡忠\n金戈鐵馬　百戰沙場　安內攘外作先鋒\n縱橫掃蕩　復興中華　所向無敵立大功"
                   "\n旌旗耀　金鼓響　龍騰虎躍軍威壯忠誠精實風紀揚\n機動攻勢　勇敢沉著　奇襲主動智謀廣\n肝膽相照　團結自強　殲滅敵寇凱歌唱")

@bot.command()
async def Lt(ctx):
    # 回覆Hello, world!
    await ctx.send("https://linktr.ee/quantx")

@bot.command()
async def Ledger(ctx):
    await ctx.send("https://docs.google.com/spreadsheets/d/12sMoS27fXK3yJX6ax1D_3BxbcnbBmTNngW_GdjsBM1s/edit#gid=0")

async def run_blocking(blocking_func, *args, **kwargs):
    """Runs a blocking function in a non-blocking way"""
    func = functools.partial(blocking_func, *args, **kwargs) # `run_in_executor` doesn't support kwargs, `functools.partial` does
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, func)

@bot.command()
async def Qs(ctx):
    global qs_in_progress
    if qs_in_progress:
        await ctx.send('command is running, plz try again later')
        return

    qs_in_progress = True
    try:
        await ctx.send("quantx strategy for main pool start")
        load_dotenv("./quantxlab/.env")
        apiKeySecretDict = {"key": [], "secret": []}

        apiKeySecretDict["key"].append(os.getenv("api_key_Tony"))
        apiKeySecretDict["secret"].append(os.getenv("secret_key_Tony"))

        apiKeySecretDict["key"].append(os.getenv("api_key_Jason"))
        apiKeySecretDict["secret"].append(os.getenv("secret_key_Jason"))

        apiKeySecretDict["key"].append(os.getenv("api_key_Steven"))
        apiKeySecretDict["secret"].append(os.getenv("secret_key_Steven"))
        messages = await run_blocking(stock_list_quantx_ds, pool="main", apiKeySecretDict=apiKeySecretDict, test=False)
        for message in messages:
            await ctx.send(message)
    finally:
        qs_in_progress = False
@bot.command()
async def Qsboyd(ctx):
    global qsboyd_in_progress
    if qsboyd_in_progress:
        await ctx.send('command is running, plz try again later')
        return

    qsboyd_in_progress = True
    try:
        await ctx.send("quantx strategy for boyd pool start")
        load_dotenv("./quantxlab/.env")
        apiKeySecretDict = {"key": [], "secret": []}

        apiKeySecretDict["key"].append(os.getenv("api_key_Tony"))
        apiKeySecretDict["secret"].append(os.getenv("secret_key_Tony"))

        apiKeySecretDict["key"].append(os.getenv("api_key_Jason"))
        apiKeySecretDict["secret"].append(os.getenv("secret_key_Jason"))
        messages = await run_blocking(stock_list_quantx_ds, pool="boyd", apiKeySecretDict=apiKeySecretDict, test=False)
        for message in messages:
            await ctx.send(message)
    finally:
        qsboyd_in_progress = False

@bot.command()
async def Nv(ctx):
    await ctx.send('Calculating Pnl...')
    load_dotenv("./quantxlab/.env")
    apiKeySecretDict = {"key": [], "secret": []}

    apiKeySecretDict["key"].append(os.getenv("api_key_Tony"))
    apiKeySecretDict["secret"].append(os.getenv("secret_key_Tony"))

    apiKeySecretDict["key"].append(os.getenv("api_key_Jason"))
    apiKeySecretDict["secret"].append(os.getenv("secret_key_Jason"))

    apiKeySecretDict["key"].append(os.getenv("api_key_Steven"))
    apiKeySecretDict["secret"].append(os.getenv("secret_key_Steven"))
    message = await run_blocking(net_worth_ds, apiKeySecretDict=apiKeySecretDict, test=False)
    await ctx.send(message)

@bot.command()
async def Inv(ctx):
    await ctx.send('Calculating Pnl and send it to Trading Engine...')
    load_dotenv("./quantxlab/.env")
    trading_engine_token = os.getenv("trading_engine_token")
    linebot_token = [trading_engine_token]
    apiKeySecretDict = {"key": [], "secret": []}

    apiKeySecretDict["key"].append(os.getenv("api_key_Tony"))
    apiKeySecretDict["secret"].append(os.getenv("secret_key_Tony"))

    apiKeySecretDict["key"].append(os.getenv("api_key_Jason"))
    apiKeySecretDict["secret"].append(os.getenv("secret_key_Jason"))

    apiKeySecretDict["key"].append(os.getenv("api_key_Steven"))
    apiKeySecretDict["secret"].append(os.getenv("secret_key_Steven"))
    await run_blocking(net_worth_linenotify, apiKeySecretDict=apiKeySecretDict, linebot=linebot_token, test=False)
    await ctx.send("notify sent")

@bot.command()
async def Dbupdate(ctx):
    global dbupdate_in_progress
    if dbupdate_in_progress:
        await ctx.send('command is running, plz try again later')
        return

    dbupdate_in_progress = True
    try:
        await ctx.send("db update start")
        await run_blocking(crawl_data)
        await ctx.send("db update finish")
    finally:
        dbupdate_in_progress = False



load_dotenv("./quantxlab/.env")
qfx_token = os.getenv("qfx_token")
bot.run(qfx_token)