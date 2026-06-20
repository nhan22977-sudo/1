import os
import ssl
import json
import time
import threading
import secrets
import random
import base64
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import asyncio
import signal
import websockets
from rich.console import Console
from rich.table import Table
from datetime import datetime
from colorama import Fore, Style
import concurrent.futures
from requests_futures.sessions import FuturesSession

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

console = Console()

tiendeptry = {
    "total": 0,
    "connected": 0,
    "invalid": 0,
    "failed": 0,
    "start_time": None
}

khoc_to_len = asyncio.Event()
skibidi_jav = asyncio.Event()

MAX_RETRIES = 3
MAX_MSG_BEFORE_CLEAR = 80
MAX_QUEUE_SIZE = 15
SMART_RETRY = 2
REQUEST_TIMEOUT = 12
MAX_WORKERS = 3
CONNECTION_POOL_SIZE = 10
BATCH_SIZE = 20

DISCORD_API_VERSION = os.getenv('DISCORD_API_VERSION', 'v10')
DISCORD_API_BASE = f"https://discord.com/api/{DISCORD_API_VERSION}"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
]
def banner():
    print(Fore.BLACK + r"""
⢀⣠⣴⣖⣺⣿⣍⠙⠛⠒⠦⣤⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠⡤⠖⠚⠋⠉⣿⣟⣒⣶⣤⣀
⠙⠉⠉⠉⠉⠙⠛⢶⣶⡦⠀⠀⠉⠳⣤⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡴⠛⠁⠀⠀⣶⣶⠞⠛⠉⠉⠉⠉⠙
⠀⠀⠀⠀⠀⠀⠀⠀⠈⠛⢿⣟⣀⡀⠈⠳⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡴⠋⠀⢀⢐⣿⠟⠋⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⢿⡟⠀⠀⠘⢷⡀⠀⠀⠀⠀⡀⠀⠀⠀⠀⠀⠀⠀⠀⣰⠟⠁⠀⠘⣿⠟⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠻⣿⠃⠀⠀⢻⣆⠼⣷⣤⣇⣱⣶⣸⣧⣴⡦⢔⣶⠃⠀⠀⢻⡿⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣿⠿⠉⠛⣿⣷⣿⣿⣿⣿⣼⣿⣿⣿⣷⣿⡟⠋⠹⣿⡟⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣸⣦⡶⠟⠛⠛⠿⠿⠋⠀⠀⠈⠻⠿⠟⠋⠛⠷⣦⣞⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣴⠟⠛⢻⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣼⠛⠛⠷⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⠟⠀⠀⠀⠈⣷⠀⣀⣀⡀⠀⠀⠀⠀⠀⠀⢀⣀⣤⡀⢰⡏⠀⠀⠀⠈⠳⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⠞⣁⣠⣤⣤⡤⠴⣿⠀⢸⣨⣿⣧⣀⣀⣀⣀⣠⣾⣧⣸⠀⢸⡷⠦⣤⣤⣤⣄⡘⢦⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⣀⡤⣞⣷⣾⣿⠿⠛⠁⠀⠀⣿⡀⠀⠛⠿⠿⠋⣉⢋⣉⠙⠿⠟⠃⠀⣸⡇⠀⠀⠙⠻⢿⣿⣶⣝⣦⣄⡀⠀⠀⠀⠀
⠀⠀⠈⠛⠛⠛⠓⠛⠛⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠘⢷⡀⠠⣀⠀⠀⠈⡟⠁⠀⢀⡠⠀⣰⠟⠀⠀⠀⠀⠀⠀⠀⠉⠙⠛⠛⠛⠛⠛⠋⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⢦⡈⢳⡀⠀⠁⠀⡰⠋⣰⠞⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣤⣤⣾⣿⡖⠃⠀⠀⠀⠃⣾⣿⣦⣤⣄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣴⣿⣿⣿⣿⣿⣿⣿⣟⠓⢶⣴⠞⠚⣿⣿⣿⣿⣿⣿⣷⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠤⣀⣠⣾⣿⣿⣿⣿⣿⣿⣿⡛⠲⣶⣿⡶⠚⣹⣿⣿⣿⣿⣿⣿⣦⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠒⠒⠋⣹⣿⠟⢹⣿⣿⣿⣿⣿⣷⣤⣬⣥⣤⣴⣿⣿⣿⣿⣿⣿⡙⣿⣿⡉⠑⠒⠂⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠑⠂⢤⣴⣏⣾⣴⡿⣿⣿⣿⣿⣿⣿⣿⠞⠙⢾⣿⣿⣿⣻⣿⣿⣿⣧⣼⣏⣷⣤⠄⠂⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠒⠒⠚⠉⠀⢹⢋⡿⠉⢹⢻⡟⣿⣿⣯⢿⣆⢀⣾⢯⣿⣿⡟⣿⢻⡉⠹⣏⢻⠁⠀⠙⠒⠒⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⠋⠁⢀⡴⣻⣸⡿⠿⡏⡇⠈⣿⣿⡏⠈⡛⡿⠿⣿⣘⡷⣄⡀⠉⢳⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠊⠉⠉⠉⠁⠙⢁⠞⠀⠀⡷⠁⣠⠋⡟⢣⡀⠱⡇⠀⠈⢆⠙⠁⠉⠉⠉⠉⠒⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡠⠖⠁⠀⠀⢠⡿⠚⠁⠀⠀⠀⠙⠲⣤⠀⠀⠀⠑⠢⢄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡜⠀⠀⠀⠀⠀⠀⠀⠀⠑⠄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
""" + Style.RESET_ALL)

def cls():
    os.system('cls' if os.name == 'nt' else 'clear')

def init_msg():
    print(Fore.CYAN + '«tool spam discord v7.5by locus»' + Style.RESET_ALL)
    print()

def check_token(token):
    if not token or len(token.strip()) < 50:
        return False

    headers = {
        "Authorization": token.strip(),
        "User-Agent": random.choice(USER_AGENTS),
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(
            f"{DISCORD_API_BASE}/users/@me",
            headers=headers,
            timeout=12,
            verify=False
        )

        if response.status_code == 200:
            try:
                data = response.json()
                return 'id' in data and 'username' in data
            except:
                return False
        elif response.status_code == 401:
            return False
        elif response.status_code == 403:
            return True
        elif response.status_code == 429:
            retry_after = response.json().get('retry_after', 1)
            time.sleep(retry_after)
            try:
                retry_response = requests.get(
                    f"{DISCORD_API_BASE}/users/@me",
                    headers=headers,
                    timeout=12,
                    verify=False
                )
                return retry_response.status_code in [200, 403]
            except:
                return False
        else:
            return False

    except requests.exceptions.Timeout:
        return False
    except requests.exceptions.ConnectionError:
        return False
    except Exception:
        return False

def filter_valid_tokens(tokens):
    valid_tokens = []
    print(f"{Fore.CYAN}Đang kiểm tra {len(tokens)} token...{Style.RESET_ALL}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        future_to_token = {executor.submit(check_token, token): token for token in tokens}
        for future in concurrent.futures.as_completed(future_to_token):
            token = future_to_token[future]
            try:
                if future.result():
                    valid_tokens.append(token)
            except:
                pass

    print(f"{Fore.CYAN}Token hợp lệ: {len(valid_tokens)}/{len(tokens)}{Style.RESET_ALL}")
    return valid_tokens

class nexusxlocus:
    def __init__(self, files):
        self.messages = []
        self.load_messages(files)

    def load_messages(self, files):
        for file_path in files:
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        self.messages.append(content)

    def get_random_message(self):
        if not self.messages:
            return "Default message"
        return random.choice(self.messages)

def get_channel_info(channel_id, token):
    headers = {
        "Authorization": token,
        "User-Agent": random.choice(USER_AGENTS)
    }
    try:
        response = requests.get(f"{DISCORD_API_BASE}/channels/{channel_id}", headers=headers, timeout=5)
        if response.status_code == 200:
            data = response.json()
            return data.get('name', 'Unknown')
        return 'Unknown'
    except:
        return 'Unknown'

class locusnexus:
    def __init__(self, message_cache, channels, tokens):
        self.message_cache = message_cache
        self.channels = channels
        self.tokens = tokens
        self.running = True
        self.batch_sessions = {}

    def create_batch_session(self, batch_tokens):
        session = FuturesSession(max_workers=len(batch_tokens))
        retry_strategy = Retry(
            total=MAX_RETRIES,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            backoff_factor=0.5
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=CONNECTION_POOL_SIZE,
            pool_maxsize=CONNECTION_POOL_SIZE
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _generate_headers(self, token):
        d = secrets.token_hex(16)
        s = secrets.token_hex(32)

        build_number = random.randint(240000, 250000)

        dev = {
            "os": "Windows",
            "browser": "Chrome",
            "device": "",
            "system_locale": random.choice(["en-US", "en-GB"]),
            "browser_user_agent": random.choice(USER_AGENTS),
            "browser_version": "120.0.0.0",
            "os_version": "10",
            "referrer": "https://discord.com/",
            "referring_domain": "discord.com",
            "release_channel": "stable",
            "client_build_number": build_number,
            "client_event_source": None
        }

        return {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": dev['browser_user_agent'],
            "X-Super-Properties": base64.b64encode(json.dumps(dev, separators=(',', ':')).encode()).decode(),
            "Origin": "https://discord.com",
            "Referer": "https://discord.com/channels/@me"
        }

    def _send_message(self, session, token, channel, message):
        nonce = str(int(time.time() * 1000) + random.randint(1000, 9999))
        payload = {
            "content": message,
            "tts": False,
            "nonce": nonce,
            "flags": 0
        }

        headers = self._generate_headers(token)
        url = f"{DISCORD_API_BASE}/channels/{channel}/messages"

        for attempt in range(SMART_RETRY):
            try:
                response = session.post(url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)

                if response.status_code == 429:
                    retry_after = 1.5
                    try:
                        retry_after = float(response.json().get('retry_after', retry_after))
                    except Exception:
                        pass
                    time.sleep(retry_after)
                    continue

                if response.status_code == 401:
                    return False

                if response.status_code == 403:
                    return False

                if 200 <= response.status_code < 300:
                    return True

            except Exception as e:
                time.sleep(random.uniform(0.8, 1.2))

        return False

    def batch_send(self, token, delay):
        session = requests.Session()
        retry_strategy = Retry(
            total=MAX_RETRIES,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            backoff_factor=0.5
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        channel_index = 0

        while self.running:
            try:
                channel = self.channels[channel_index]
                message = self.message_cache.get_random_message()

                self._send_message(session, token, channel, message)

                channel_index = (channel_index + 1) % len(self.channels)
                time.sleep(delay)

            except Exception:
                time.sleep(1.5)

        session.close()

def print_channel_info(channels, token):
    print(f"{Fore.CYAN}Spammer Coming In {len(channels)} Channel{Style.RESET_ALL}")
    for i, channel in enumerate(channels, 1):
        channel_name = get_channel_info(channel, token)
        print(f"{Fore.CYAN}{i} >> Id: {channel} - Name: {channel_name}{Style.RESET_ALL}")
    print()

def get_message_files():
    files = []
    print(f"{Fore.CYAN}Nhập file tin nhắn ( 'done' để dừng):{Style.RESET_ALL}")

    while True:
        file_path = input(f"{Fore.CYAN}File: {Style.RESET_ALL}").strip()
        if file_path.lower() == 'done':
            break
        if file_path and os.path.exists(file_path):
            files.append(file_path)
        elif file_path:
            pass

    return files

class NhayV2:
    def __init__(self, file_path, tag_id):
        with open(file_path, 'r', encoding='utf-8') as f:
            self.lines = [line.strip() for line in f if line.strip()]
        self.tag = f" <@{tag_id}>" if tag_id else ""

    def get_random_message(self):
        if not self.lines:
            return "Default message" + self.tag
        return random.choice(self.lines) + self.tag


def sigmaboi(sig, frame):
    khoc_to_len.set()


def token(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as bruh:
            return [line.strip() for line in bruh if line.strip()]
    except FileNotFoundError:
        console.print("[bold red]Không tìm thấy file![/bold red]")
        return []


def tb_stt():
    panel = Table(show_header=True, header_style="bold cyan")
    panel.add_column("Info", style="cyan", width=20)
    panel.add_column("Status", style="green", width=30)

    if tiendeptry["start_time"]:
        runtime = datetime.now() - tiendeptry["start_time"]
        hrs, leftover = divmod(int(runtime.total_seconds()), 3600)
        mins, secs = divmod(leftover, 60)
        time_str = f"{hrs:02d}:{mins:02d}:{secs:02d}"
    else:
        time_str = "00:00:00"

    panel.add_row("Uptime", time_str)
    panel.add_row("Tổng token", str(tiendeptry["total"]))
    panel.add_row("Đã kết nối", f"[green]{tiendeptry['connected']}[/green]")
    panel.add_row("Không hợp lệ", f"[red]{tiendeptry['invalid']}[/red]")
    panel.add_row("Lỗi kết nối", f"[yellow]{tiendeptry['failed']}[/yellow]")

    return panel


def dpl():
    os.system('cls' if os.name == 'nt' else 'clear')
    console.print(tb_stt())


async def get(auth):
    try:
        res = requests.get(
            "https://discord.com/api/v10/users/@me",
            headers={"Authorization": auth},
            timeout=10
        )
        if res.status_code == 200:
            return res.json().get("id")
        elif res.status_code == 401:
            tiendeptry["invalid"] += 1
    except Exception as e:
        console.print(f"[red]Lỗi get user: {e}[/red]")
    return None


async def mbt_cdj(sock, stuff):
    try:
        await sock.send(json.dumps(stuff))
        return True
    except Exception:
        return False


async def heartbeat(sock, delay, alive):
    try:
        jitter = random.uniform(0, 0.1)
        await asyncio.sleep(delay * jitter)
        while alive[0] and not khoc_to_len.is_set():
            try:
                if not await mbt_cdj(sock, {"op": 1, "d": None}):
                    await asyncio.sleep(1)
                    continue
                await asyncio.sleep(delay)
            except Exception:
                await asyncio.sleep(1)
                if not alive[0]:
                    break
                continue
    except Exception:
        pass


async def get_channel_guild(channel_id, auth):
    try:
        res = requests.get(
            f"https://discord.com/api/v10/channels/{channel_id}",
            headers={"Authorization": auth},
            timeout=10
        )
        if res.status_code == 200:
            return res.json().get("guild_id")
    except Exception:
        pass
    return None


async def join_voice(auth, channel, mute, deaf, stream, cam):
    uid = await get(auth)
    if not uid:
        return

    guild_id = await get_channel_guild(channel, auth)
    if not guild_id:
        tiendeptry["failed"] += 1
        return

    alive = [True]
    sock = None
    last_heartbeat = [0]
    heartbeat_interval = [None]

    try:
        res = requests.get(
            "https://discord.com/api/v10/gateway",
            headers={"Authorization": auth},
            timeout=10
        )
        if res.status_code != 200:
            tiendeptry["failed"] += 1
            return

        gate = res.json().get("url") + "/?v=10&encoding=json"
        ctx = ssl.create_default_context()

        sock = await asyncio.wait_for(
            websockets.connect(gate, ssl=ctx, max_size=None, ping_interval=None),
            timeout=15
        )

        hello = await asyncio.wait_for(sock.recv(), timeout=15)
        hello_data = json.loads(hello)

        if hello_data.get("op") != 10:
            tiendeptry["failed"] += 1
            return

        heartbeat_interval[0] = hello_data["d"]["heartbeat_interval"] / 1000
        asyncio.create_task(heartbeat(sock, heartbeat_interval[0], alive))

        await mbt_cdj(sock, {
            "op": 2,
            "d": {
                "token": auth,
                "properties": {"$os": "windows", "$browser": "chrome", "$device": "pc"},
                "intents": 513
            }
        })

        ready_sent = False
        voice_joined = False

        while alive[0] and not khoc_to_len.is_set():
            try:
                msg = await asyncio.wait_for(sock.recv(), timeout=45)
                packet = json.loads(msg)

                if packet.get("op") == 0:
                    evt = packet.get("t")
                    if evt == "READY":
                        tiendeptry["connected"] += 1
                        ready_sent = True
                        await asyncio.sleep(0.3)
                        await mbt_cdj(sock, {
                            "op": 4,
                            "d": {
                                "guild_id": guild_id,
                                "channel_id": channel,
                                "self_mute": mute,
                                "self_deaf": deaf,
                                "self_video": cam
                            }
                        })
                        voice_joined = True
                        if stream:
                            await asyncio.sleep(0.5)
                            await mbt_cdj(sock, {
                                "op": 18,
                                "d": {
                                    "type": "guild",
                                    "guild_id": guild_id,
                                    "channel_id": channel,
                                    "preferred_region": None
                                }
                            })
                    elif evt == "VOICE_STATE_UPDATE":
                        if packet["d"].get("user_id") == uid and packet["d"].get("channel_id") is None:
                            if voice_joined:
                                await asyncio.sleep(random.uniform(1, 2))
                                await mbt_cdj(sock, {
                                    "op": 4,
                                    "d": {
                                        "guild_id": guild_id,
                                        "channel_id": channel,
                                        "self_mute": mute,
                                        "self_deaf": deaf,
                                        "self_video": cam
                                    }
                                })
                elif packet.get("op") == 7:
                    break
                elif packet.get("op") == 9:
                    if ready_sent:
                        await asyncio.sleep(random.uniform(1, 3))
                        await mbt_cdj(sock, {
                            "op": 2,
                            "d": {
                                "token": auth,
                                "properties": {"$os": "windows", "$browser": "chrome", "$device": "pc"},
                                "intents": 513
                            }
                        })
            except asyncio.TimeoutError:
                if ready_sent:
                    continue
                else:
                    break
            except Exception as e:
                break

    except Exception as e:
        tiendeptry["failed"] += 1
    finally:
        alive[0] = False
        if sock:
            try:
                await sock.close()
            except Exception:
                pass


async def batch(tokens, channel, mute, deaf, stream, cam, batch_size=5):
    for idx in range(0, len(tokens), batch_size):
        if khoc_to_len.is_set():
            break
        chunk = tokens[idx:idx + batch_size]
        jobs = [asyncio.create_task(join_voice(auth, channel, mute, deaf, stream, cam)) for auth in chunk]
        await asyncio.sleep(2)
        await asyncio.gather(*jobs, return_exceptions=True)
        if idx + batch_size < len(tokens):
            await asyncio.sleep(1.5)
    skibidi_jav.set()


async def ud():
    await skibidi_jav.wait()
    while not khoc_to_len.is_set():
        dpl()
        await asyncio.sleep(1)


def join_server():
    token_file = input(f"{Fore.CYAN}Nhập file chứa token (vd: token.txt): {Style.RESET_ALL}").strip()
    if not token_file:
        print(f"{Fore.RED}Bạn chưa nhập file{Style.RESET_ALL}")
        return

    try:
        with open(token_file, 'r', encoding='utf-8') as f:
            all_tokens = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"{Fore.RED}Không tìm thấy file '{token_file}'{Style.RESET_ALL}")
        return

    if not all_tokens:
        print(f"{Fore.RED}Không tìm thấy token nào trong {token_file}{Style.RESET_ALL}")
        return

    tokens = filter_valid_tokens(all_tokens)

    if not tokens:
        print(f"{Fore.RED}Không có token hợp lệ nào{Style.RESET_ALL}")
        return

    invite_input = input(f"{Fore.CYAN}Nhập link mời hoặc mã mời server (vd: https://discord.gg/abc123 hoặc abc123): {Style.RESET_ALL}").strip()
    if not invite_input:
        print(f"{Fore.RED}Bạn chưa nhập link hoặc mã mời{Style.RESET_ALL}")
        return

    # Extract invite code from input
    if 'discord.gg/' in invite_input:
        invite_code = invite_input.split('discord.gg/')[-1].split('?')[0].split('/')[0]
    elif 'discord.com/invite/' in invite_input:
        invite_code = invite_input.split('discord.com/invite/')[-1].split('?')[0].split('/')[0]
    else:
        invite_code = invite_input.split('?')[0].split('/')[0]  # Assume it's just the code

    print(f"{Fore.CYAN}Đang join server với {len(tokens)} token...{Style.RESET_ALL}")

    joined = 0
    failed = 0

    def join_with_token(token):
        nonlocal joined, failed
        headers = {
            "Authorization": token,
            "User-Agent": random.choice(USER_AGENTS)
        }

        url = f"https://discord.com/api/v10/invites/{invite_code}"

        for attempt in range(3):
            try:
                response = requests.post(url, headers=headers, timeout=10, verify=False)

                if response.status_code == 200:
                    print(f"{Fore.GREEN}Token {token[-4:]} đã join thành công{Style.RESET_ALL}")
                    joined += 1
                    return
                elif response.status_code == 400:
                    try:
                        error = response.json()
                        message = error.get('message', 'Unknown error')
                        if 'already' in message.lower() or 'joined' in message.lower() or 'đã' in message.lower():
                            print(f"{Fore.YELLOW}Token {token[-4:]} đã join rồi{Style.RESET_ALL}")
                            joined += 1
                            return
                        else:
                            print(f"{Fore.RED}Token {token[-4:]} join thất bại: {message}{Style.RESET_ALL}")
                            failed += 1
                            return
                    except ValueError:
                        # Response is not JSON
                        text = response.text
                        if 'already' in text.lower() or 'joined' in text.lower() or 'đã' in text.lower():
                            print(f"{Fore.YELLOW}Token {token[-4:]} đã join rồi{Style.RESET_ALL}")
                            joined += 1
                            return
                        else:
                            print(f"{Fore.RED}Token {token[-4:]} join thất bại: {text[:100]}{Style.RESET_ALL}")
                            failed += 1
                            return
                elif response.status_code == 429:
                    try:
                        error = response.json()
                        retry_after = error.get('retry_after', 5)
                    except ValueError:
                        retry_after = 5
                    print(f"{Fore.YELLOW}Rate limit cho token {token[-4:]}, chờ {retry_after}s{Style.RESET_ALL}")
                    time.sleep(retry_after)
                    continue
                elif response.status_code == 403:
                    print(f"{Fore.RED}Token {token[-4:]} không có quyền: 403{Style.RESET_ALL}")
                    failed += 1
                    return
                elif response.status_code == 401:
                    print(f"{Fore.RED}Token {token[-4:]} không hợp lệ: 401{Style.RESET_ALL}")
                    failed += 1
                    return
                else:
                    try:
                        error = response.json()
                        message = error.get('message', f'HTTP {response.status_code}')
                        print(f"{Fore.RED}Token {token[-4:]} join thất bại: {message}{Style.RESET_ALL}")
                    except ValueError:
                        text = response.text
                        print(f"{Fore.RED}Token {token[-4:]} join thất bại: {text[:100]}{Style.RESET_ALL}")
                    failed += 1
                    return
            except requests.exceptions.Timeout:
                print(f"{Fore.YELLOW}Timeout cho token {token[-4:]}, thử lại...{Style.RESET_ALL}")
                time.sleep(2)
                continue
            except Exception as e:
                print(f"{Fore.RED}Lỗi kết nối cho token {token[-4:]}: {e}{Style.RESET_ALL}")
                failed += 1
                return

        print(f"{Fore.RED}Token {token[-4:]} join thất bại sau 3 lần thử{Style.RESET_ALL}")
        failed += 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(join_with_token, tokens)

    print(f"{Fore.CYAN}Hoàn thành: {joined} thành công, {failed} thất bại{Style.RESET_ALL}")


async def treovoice():
    signal.signal(signal.SIGINT, sigmaboi)
    khoc_to_len.clear()
    skibidi_jav.clear()

    path = console.input("[bold yellow]File token: [/bold yellow]")
    tokens = token(path)
    if not tokens:
        console.print("[bold red]Không có token nào![/bold red]")
        return

    tiendeptry["total"] = len(tokens)
    tiendeptry["connected"] = 0
    tiendeptry["invalid"] = 0
    tiendeptry["failed"] = 0
    tiendeptry["start_time"] = datetime.now()

    channel = console.input("[bold blue]Id kenh voice: [/bold blue]")

    mute = console.input("[bold red]? Tắt mic (y/n): [/bold red]").lower() == "y"
    deaf = console.input("[bold red]? Tắt loa (y/n): [/bold red]").lower() == "y"
    stream = console.input("[bold red]? Chia sẻ màn hình (y/n): [/bold red]").lower() == "y"
    cam = console.input("[bold red]? Bật camera (y/n): [/bold red]").lower() == "y"

    console.print("\n[bold green]Loading...[/bold green]\n")

    monitor = asyncio.create_task(ud())

    await batch(tokens, channel, mute, deaf, stream, cam)

    try:
        await khoc_to_len.wait()
    except Exception:
        pass
    finally:
        console.print("\n[bold yellow]Stop !!![/bold yellow]")
        monitor.cancel()
        await asyncio.sleep(2)


def main():
    cls()
    banner()
    init_msg()

    while True:
        print(f"{Fore.CYAN}Menu chức năng:{Style.RESET_ALL}")
        print(f"{Fore.CYAN}1. Treo{Style.RESET_ALL}")
        print(f"{Fore.CYAN}2. Nhay v2{Style.RESET_ALL}")
        print(f"{Fore.CYAN}3. TreoVoice{Style.RESET_ALL}")
        print(f"{Fore.CYAN}4. Join Server{Style.RESET_ALL}")
        print(f"{Fore.CYAN}5. Thoát{Style.RESET_ALL}")
        choice = input(f"{Fore.CYAN}Chọn chức năng: {Style.RESET_ALL}").strip()

        if choice == '1':
            # Treo functionality - current spam code
            token_file = input(f"{Fore.CYAN}Nhập file chứa token: {Style.RESET_ALL}").strip()
            if not token_file:
                print(f"{Fore.RED}Bạn chưa nhập file{Style.RESET_ALL}")
                continue

            try:
                with open(token_file, 'r', encoding='utf-8') as f:
                    all_tokens = [line.strip() for line in f if line.strip()]
            except FileNotFoundError:
                print(f"{Fore.RED}Không tìm thấy file '{token_file}'{Style.RESET_ALL}")
                continue

            if not all_tokens:
                print(f"{Fore.RED}Không tìm thấy token nào trong {token_file}{Style.RESET_ALL}")
                continue

            tokens = filter_valid_tokens(all_tokens)

            if not tokens:
                print(f"{Fore.RED}Không có token hợp lệ nào{Style.RESET_ALL}")
                continue

            channel = input(f"{Fore.CYAN}Nhập ID kênh: {Style.RESET_ALL}").strip()
            if not channel:
                print(f"{Fore.RED}Bạn chưa nhập ID kênh{Style.RESET_ALL}")
                continue
            channels = [channel]

            message_file = input(f"{Fore.CYAN}Nhập file tin nhắn: {Style.RESET_ALL}").strip()
            if not message_file or not os.path.exists(message_file):
                print(f"{Fore.RED}File tin nhắn không tồn tại{Style.RESET_ALL}")
                continue
            message_files = [message_file]

            message_cache = nexusxlocus(message_files)

            delays = {}
            for token in tokens:
                while True:
                    try:
                        delay = float(input(f"{Fore.CYAN}Delay cho token {token[-4:]} [Giây]: {Style.RESET_ALL}"))
                        delays[token] = delay
                        break
                    except ValueError:
                        print(f"{Fore.RED}Vui lòng nhập số hợp lệ{Style.RESET_ALL}")

            lich = locusnexus(message_cache, channels, tokens)

            print(f'{Fore.CYAN}Đang khởi động...{Style.RESET_ALL}')
            time.sleep(2)
            cls()
            banner()
            init_msg()
            print_channel_info(channels, tokens[0])

            threads = []
            for token in tokens:
                thread = threading.Thread(
                    target=lich.batch_send,
                    args=(token, delays[token]),
                    daemon=True
                )
                threads.append(thread)
                thread.start()

            try:
                while True:
                    time.sleep(60)
            except KeyboardInterrupt:
                print(f"{Fore.CYAN}Đang dừng chương trình...{Style.RESET_ALL}")
                lich.running = False
                break

        elif choice == '2':
            # Nhay v2 functionality
            token_file = input(f"{Fore.CYAN}Nhập file chứa token: {Style.RESET_ALL}").strip()
            if not token_file:
                print(f"{Fore.RED}Bạn chưa nhập file{Style.RESET_ALL}")
                continue

            try:
                with open(token_file, 'r', encoding='utf-8') as f:
                    all_tokens = [line.strip() for line in f if line.strip()]
            except FileNotFoundError:
                print(f"{Fore.RED}Không tìm thấy file '{token_file}'{Style.RESET_ALL}")
                continue

            if not all_tokens:
                print(f"{Fore.RED}Không tìm thấy token nào trong {token_file}{Style.RESET_ALL}")
                continue

            tokens = filter_valid_tokens(all_tokens)

            if not tokens:
                print(f"{Fore.RED}Không có token hợp lệ nào{Style.RESET_ALL}")
                continue

            channel = input(f"{Fore.CYAN}Nhập ID kênh: {Style.RESET_ALL}").strip()
            if not channel:
                print(f"{Fore.RED}Bạn chưa nhập ID kênh{Style.RESET_ALL}")
                continue
            channels = [channel]

            message_file = input(f"{Fore.CYAN}Nhập file tin nhắn: {Style.RESET_ALL}").strip()
            if not message_file or not os.path.exists(message_file):
                print(f"{Fore.RED}File tin nhắn không tồn tại{Style.RESET_ALL}")
                continue

            tag_id = input(f"{Fore.CYAN}Nhập ID người dùng để tag (Enter nếu không tag): {Style.RESET_ALL}").strip()
            message_cache = NhayV2(message_file, tag_id)

            delays = {}
            for token in tokens:
                while True:
                    try:
                        delay = float(input(f"{Fore.CYAN}Delay cho token {token[-4:]} [Giây]: {Style.RESET_ALL}"))
                        delays[token] = delay
                        break
                    except ValueError:
                        print(f"{Fore.RED}Vui lòng nhập số hợp lệ{Style.RESET_ALL}")

            lich = locusnexus(message_cache, channels, tokens)

            print(f'{Fore.CYAN}Đang khởi động...{Style.RESET_ALL}')
            time.sleep(2)
            cls()
            banner()
            init_msg()
            print_channel_info(channels, tokens[0])

            threads = []
            for token in tokens:
                thread = threading.Thread(
                    target=lich.batch_send,
                    args=(token, delays[token]),
                    daemon=True
                )
                threads.append(thread)
                thread.start()

            try:
                while True:
                    time.sleep(60)
            except KeyboardInterrupt:
                print(f"{Fore.CYAN}Đang dừng chương trình...{Style.RESET_ALL}")
                lich.running = False
                break
        elif choice == '3':
            try:
                asyncio.run(treovoice())
            except KeyboardInterrupt:
                pass
        elif choice == '4':
            join_server()
        elif choice == '5':
            print(f"{Fore.CYAN}Thoát chương trình.{Style.RESET_ALL}")
            break
        else:
            print(f"{Fore.RED}Chức năng không hợp lệ. Vui lòng chọn lại.{Style.RESET_ALL}")

if __name__ == "__main__":
    main()
