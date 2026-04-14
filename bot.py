import telebot
from telebot.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
import requests
import sqlite3
import datetime
import os
import logging
import random
from contextlib import closing

# ================= LOGGING =================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ================= CONFIG =================
from dotenv import load_dotenv
load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
print("MY TOKEN IS:", TELEGRAM_TOKEN) # <--- ADD THIS LINE

WAQI_TOKEN     = os.getenv('WAQI_TOKEN')       # https://aqicn.org/data-platform/token/
OWM_TOKEN      = os.getenv('OWM_TOKEN')         # https://openweathermap.org/api

# ⚠️  Replace 000000000 with YOUR real Telegram user ID
MY_OWN_ID   = 7823754470
FRIEND_ID   = 1239971862   # @Nishonov_Ulugbek
ADMIN_IDS   = [MY_OWN_ID, FRIEND_ID]

LOG_GROUP_ID = -5197077622  # Your log group
DB_NAME      = 'bot_database.db'

bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None

# ================= DATABASE =================
class DB:
    @staticmethod
    def run(query, params=(), commit=False, fetchone=False, fetchall=False):
        try:
            with sqlite3.connect(DB_NAME, timeout=10) as conn:
                with closing(conn.cursor()) as c:
                    c.execute(query, params)
                    if commit:
                        conn.commit()
                    if fetchone:
                        return c.fetchone()
                    if fetchall:
                        return c.fetchall()
        except sqlite3.Error as e:
            logger.error(f"DB error: {e} | Query: {query}")
            return None

def init_db():
    DB.run('''CREATE TABLE IF NOT EXISTS users (
        chat_id         INTEGER PRIMARY KEY,
        lang            TEXT    DEFAULT 'en',
        referred_by     INTEGER,
        referrals       INTEGER DEFAULT 0,
        username        TEXT,
        first_name      TEXT,
        last_active_date TEXT
    )''', commit=True)
    # Safe migrations
    for col in ["username TEXT", "first_name TEXT", "last_active_date TEXT"]:
        try:
            DB.run(f"ALTER TABLE users ADD COLUMN {col}", commit=True)
        except Exception:
            pass
    logger.info("✅ Database ready.")

init_db()

# ================= USER HELPERS =================
def get_or_create_user(user_obj, referrer_id=None):
    uid      = user_obj.id
    username = user_obj.username  or ""
    fname    = user_obj.first_name or ""
    today    = datetime.date.today().isoformat()

    row = DB.run("SELECT lang, referrals FROM users WHERE chat_id=?", (uid,), fetchone=True)
    if not row:
        DB.run(
            "INSERT INTO users (chat_id,lang,referred_by,referrals,username,first_name,last_active_date) VALUES (?,?,?,?,?,?,?)",
            (uid, 'en', referrer_id, 0, username, fname, today), commit=True
        )
        if referrer_id and referrer_id != uid:
            DB.run("UPDATE users SET referrals=referrals+1 WHERE chat_id=?", (referrer_id,), commit=True)
            try:
                bot.send_message(referrer_id, "🎉 A new friend joined via your link! You gained 1 referral.")
            except Exception as e:
                logger.warning(f"Referral notify failed: {e}")
        return 'en', 0
    else:
        DB.run(
            "UPDATE users SET username=?,first_name=?,last_active_date=? WHERE chat_id=?",
            (username, fname, today, uid), commit=True
        )
        return row[0], row[1]

def get_lang(uid):
    row = DB.run("SELECT lang FROM users WHERE chat_id=?", (uid,), fetchone=True)
    return row[0] if row else 'en'

def set_lang(uid, lang):
    DB.run("UPDATE users SET lang=? WHERE chat_id=?", (lang, uid), commit=True)

# ================= TRANSLATIONS =================
T = {
    'en': {
        'lang_btn':       '🇬🇧 English',
        'choose_lang':    '🌍 Choose your language:',
        'main_menu':      '🏠 *Main Menu* — What would you like to check?',
        'btn_geo':        '📍 My Location',
        'btn_region':     '🗺️ Regions',
        'btn_game':       '🎮 Game',
        'btn_stat':       '📊 Statistics',
        'back':           '« Back',
        'fetching':       '⏳ Fetching live data...',
        'geo_req':        '📍 Tap the button below to share your location.',
        'send_loc_btn':   '📍 Share My Location',
        'choose_region':  '🗺️ *Select a Region:*',
        'choose_district':'📍 *Select a District in {region}:*',
        'stat_menu':      '📊 *Statistics*\nYour total invites: *{inv}*',
        'top_users_btn':  '🏆 Top Users ⚽️',
        'share_btn':      '🔗 Share with Friends',
        'add_group_btn':  '➕ Add to a Group',
        'feedback_btn':   '💬 Feedback',
        'num_users_btn':  '👥 Number of Users',
        'active_btn':     '🔥 Active Users',
        'admin_btn':      '🛡️ Admin Panel',
        'game_locked':    '🔒 Invite at least 1 friend to unlock the game! Go to Statistics → Share.',
        'game_temp':      '🌡️ *Step 1/3 — Pick a Temperature (°C):*',
        'game_wind':      '💨 *Step 2/3 — Pick a Wind Speed (m/s):*',
        'game_hum':       '💧 *Step 3/3 — Pick a Humidity (%):*',
        'game_result':    (
            '🧠 *Your Prediction Result*\n\n'
            '🌡️ Temp: *{temp}°C*\n'
            '💨 Wind: *{wind} m/s*\n'
            '💧 Humidity: *{hum}%*\n\n'
            '🔬 Predicted AQI Level: *{level}*\n\n'
            '_Keep monitoring real data for accuracy!_'
        ),
        'hourly_btn':     '⏱️ Today\'s Hourly',
        'tomorrow_btn':   '📅 Tomorrow\'s Forecast',
        'hourly_title':   '⏱️ *Today\'s Hourly Forecast — {name}*\n\n',
        'tomorrow_title': '📅 *Tomorrow\'s Forecast — {name}*\n\n',
        'hourly_row':     '🕒 *{t}:00* | 🌡️ {temp}°C | 💨 {wind}m/s | 💧 {hum}% | ☁️ _{desc}_\n',
        'top_title':      '🏆 *Top Users — Ballon d\'Or ⚽️*\n\n',
        'top_row':        '{medal} *{name}* {uname}— *{inv}* invites\n',
        'top_footer':     '\n📌 Your rank: *#{rank}* with *{inv}* invites.',
        'feedback_post':  (
            '━━━━━━━━━━━━━━━━━━━━\n'
            '💬 *Got feedback?*\n\n'
            'If you have any feedback regarding the bot, pls 🙏 contact\n'
            '👤 *@Iht_student*\n\n'
            'I read every message — bug reports, ideas, roasts, all welcome 😄\n'
            '━━━━━━━━━━━━━━━━━━━━'
        ),
        'no_data':        '⚠️ Could not fetch data. Please try again later.',
    },
    'ru': {
        'lang_btn':       '🇷🇺 Русский',
        'choose_lang':    '🌍 Выберите язык:',
        'main_menu':      '🏠 *Главное меню* — Что проверим?',
        'btn_geo':        '📍 Моя локация',
        'btn_region':     '🗺️ Регионы',
        'btn_game':       '🎮 Игра',
        'btn_stat':       '📊 Статистика',
        'back':           '« Назад',
        'fetching':       '⏳ Получаем данные...',
        'geo_req':        '📍 Нажмите кнопку ниже, чтобы поделиться локацией.',
        'send_loc_btn':   '📍 Поделиться локацией',
        'choose_region':  '🗺️ *Выберите регион:*',
        'choose_district':'📍 *Выберите район в {region}:*',
        'stat_menu':      '📊 *Статистика*\nВаших приглашений: *{inv}*',
        'top_users_btn':  '🏆 Топ Пользователей ⚽️',
        'share_btn':      '🔗 Поделиться с друзьями',
        'add_group_btn':  '➕ Добавить в группу',
        'feedback_btn':   '💬 Обратная связь',
        'num_users_btn':  '👥 Число пользователей',
        'active_btn':     '🔥 Активные пользователи',
        'admin_btn':      '🛡️ Панель администратора',
        'game_locked':    '🔒 Пригласите хотя бы 1 друга, чтобы разблокировать игру! Статистика → Поделиться.',
        'game_temp':      '🌡️ *Шаг 1/3 — Выберите температуру (°C):*',
        'game_wind':      '💨 *Шаг 2/3 — Выберите скорость ветра (м/с):*',
        'game_hum':       '💧 *Шаг 3/3 — Выберите влажность (%):*',
        'game_result':    (
            '🧠 *Ваш прогноз*\n\n'
            '🌡️ Темп: *{temp}°C*\n'
            '💨 Ветер: *{wind} м/с*\n'
            '💧 Влажность: *{hum}%*\n\n'
            '🔬 Предполагаемый ИКВ: *{level}*\n\n'
            '_Следите за реальными данными!_'
        ),
        'hourly_btn':     '⏱️ Сегодня почасово',
        'tomorrow_btn':   '📅 Прогноз на завтра',
        'hourly_title':   '⏱️ *Почасовой прогноз сегодня — {name}*\n\n',
        'tomorrow_title': '📅 *Прогноз на завтра — {name}*\n\n',
        'hourly_row':     '🕒 *{t}:00* | 🌡️ {temp}°C | 💨 {wind}м/с | 💧 {hum}% | ☁️ _{desc}_\n',
        'top_title':      '🏆 *Топ Пользователей — Золотой мяч ⚽️*\n\n',
        'top_row':        '{medal} *{name}* {uname}— *{inv}* приглаш.\n',
        'top_footer':     '\n📌 Ваш ранг: *#{rank}* ({inv} приглаш.)',
        'feedback_post':  (
            '━━━━━━━━━━━━━━━━━━━━\n'
            '💬 *Есть отзыв?*\n\n'
            'Если есть замечания по боту, пожалуйста 🙏 пишите\n'
            '👤 *@Iht_student*\n\n'
            'Читаю каждое сообщение — баги, идеи, всё принимается 😄\n'
            '━━━━━━━━━━━━━━━━━━━━'
        ),
        'no_data':        '⚠️ Не удалось получить данные. Попробуйте позже.',
    },
    'uz': {
        'lang_btn':       "🇺🇿 O'zbekcha",
        'choose_lang':    '🌍 Tilni tanlang:',
        'main_menu':      "🏠 *Asosiy Menyu* — Nima tekshiramiz?",
        'btn_geo':        '📍 Mening joylashuvim',
        'btn_region':     '🗺️ Viloyatlar',
        'btn_game':       "🎮 O'yin",
        'btn_stat':       '📊 Statistika',
        'back':           '« Orqaga',
        'fetching':       "⏳ Ma'lumot olinmoqda...",
        'geo_req':        '📍 Joylashuvingizni ulashish uchun quyidagi tugmani bosing.',
        'send_loc_btn':   '📍 Joylashuvni ulashish',
        'choose_region':  '🗺️ *Viloyatni tanlang:*',
        'choose_district':"📍 *{region} tumanlaridan birini tanlang:*",
        'stat_menu':      "📊 *Statistika*\nTakliflaringiz: *{inv}*",
        'top_users_btn':  "🏆 Top Foydalanuvchilar ⚽️",
        'share_btn':      "🔗 Do'stlar bilan ulashish",
        'add_group_btn':  "➕ Guruhga qo'shish",
        'feedback_btn':   '💬 Fikr-mulohaza',
        'num_users_btn':  "👥 Foydalanuvchilar soni",
        'active_btn':     '🔥 Faol foydalanuvchilar',
        'admin_btn':      '🛡️ Admin Panel',
        'game_locked':    "🔒 O'yin ochish uchun kamida 1 do'stingizni taklif qiling! Statistika → Ulashish.",
        'game_temp':      '🌡️ *1/3-qadam — Haroratni tanlang (°C):*',
        'game_wind':      '💨 *2/3-qadam — Shamol tezligini tanlang (m/s):*',
        'game_hum':       '💧 *3/3-qadam — Namlikni tanlang (%):*',
        'game_result':    (
            '🧠 *Sizning bashoratingiz*\n\n'
            '🌡️ Harorat: *{temp}°C*\n'
            '💨 Shamol: *{wind} m/s*\n'
            '💧 Namlik: *{hum}%*\n\n'
            "🔬 Taxminiy havo sifati: *{level}*\n\n"
            "_Haqiqiy ma'lumotlarni kuzatib boring!_"
        ),
        'hourly_btn':     '⏱️ Bugungi soatlik',
        'tomorrow_btn':   "📅 Ertangi ob-havo",
        'hourly_title':   "⏱️ *Bugungi soatlik ob-havo — {name}*\n\n",
        'tomorrow_title': "📅 *Ertangi ob-havo — {name}*\n\n",
        'hourly_row':     '🕒 *{t}:00* | 🌡️ {temp}°C | 💨 {wind}m/s | 💧 {hum}% | ☁️ _{desc}_\n',
        'top_title':      "🏆 *Top Foydalanuvchilar — Oltin to'p ⚽️*\n\n",
        'top_row':        '{medal} *{name}* {uname}— *{inv}* taklif\n',
        'top_footer':     '\n📌 Sizning o\'rningiz: *#{rank}* ({inv} taklif)',
        'feedback_post':  (
            '━━━━━━━━━━━━━━━━━━━━\n'
            '💬 *Fikringiz bormi?*\n\n'
            "Bot haqida takliflaringiz bo'lsa, iltimos 🙏 yozing:\n"
            '👤 *@Iht_student*\n\n'
            "Har bir xabarni o'qiyman — xato, g'oya, hammasi qabul 😄\n"
            '━━━━━━━━━━━━━━━━━━━━'
        ),
        'no_data':        "⚠️ Ma'lumot olishda xato. Keyinroq urinib ko'ring.",
    }
}

def t(uid, key, **kwargs):
    lang = get_lang(uid)
    val  = T.get(lang, T['en']).get(key, T['en'].get(key, key))
    return val.format(**kwargs) if kwargs else val

def back_btn(uid, cb):
    return InlineKeyboardButton(t(uid, 'back'), callback_data=cb)

# ================= REGIONS =================
REGIONS = {
    'Tashkent City':    ['Yunusabad','Mirzo Ulugbek','Chilonzor','Yashnabad','Bektemir',
                         'Yakkasaray','Mirobod','Uchtepa','Almazar','Shaykhontohur','Sergeli','Yangihayot'],
    'Tashkent Region':  ['Angren','Buka','Chirchiq','Parkent','Zangiota','Kibray','Yangiyul','Nurafshon'],
    'Andijan':          ['Andijan City','Asaka','Shahrixon','Xonobod',"Oltinko'l",'Buloqboshi'],
    'Bukhara':          ['Bukhara City','Gijduvon','Jondor','Kogon','Peshku','Romitan','Shofirkon'],
    'Jizzakh':          ['Jizzakh City','Arnasoy','Baxmal',"Do'stlik",'Forish',"G'allaorol",'Zomin'],
    'Kashkadarya':      ['Karshi','Kason','Kitob','Muborak','Shakhrisabz','Guzar','Kamashi'],
    'Navoi':            ['Navoi City','Khatyrchi','Kyzyltepa','Nurata','Uchkuduk','Zarafshan'],
    'Namangan':         ['Namangan City','Chust','Kosonsoy','Pop','Turakurgan','Uychi','Davlatobod'],
    'Samarkand':        ['Samarkand City','Bulungur','Ishtixan',"Kattaqo'rg'on",'Narpay','Pastdargom','Urgut','Toyloq'],
    'Surkhandarya':     ['Termez','Boysun','Denov',"Jarqo'rg'on",'Kumqurghon','Sariosiyo','Sherobod'],
    'Syrdarya':         ['Guliston','Sirdaryo','Yangiyer','Shirin','Oqoltin','Xavos'],
    'Fergana':          ['Fergana City','Margilan','Kokand','Kuva','Rishton','Oltiariq','Beshariq'],
    'Khorezm':          ['Urgench','Khiva','Gurlen',"Xonqa","Qo'shko'pir",'Shovot','Hazorasp'],
    'Karakalpakstan':   ['Nukus','Amudaryo','Beruniy','Chimboy','Ellikqala','Kegeyli',"Qo'ng'irot","Mo'ynoq"]
}

# ================= WEATHER API =================
def geocode(query):
    """Returns (lat, lon) or (None, None)."""
    try:
        url = f"http://api.openweathermap.org/geo/1.0/direct?q={query}&limit=1&appid={OWM_TOKEN}"
        res = requests.get(url, timeout=6).json()
        if res:
            return res[0]['lat'], res[0]['lon']
    except Exception as e:
        logger.error(f"Geocode error: {e}")
    return None, None

def fetch_current(lat, lon):
    """Returns dict with temp/hum/wind/aqi or None."""
    try:
        w = requests.get(
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?lat={lat}&lon={lon}&appid={OWM_TOKEN}&units=metric", timeout=6
        ).json()
        temp = round(w['main']['temp'], 1)
        hum  = w['main']['humidity']
        wind = round(w['wind']['speed'], 1)
        desc = w['weather'][0]['description'].capitalize()
    except Exception as e:
        logger.error(f"OWM current failed: {e}")
        return None

    # AQI — WAQI first, OWM fallback
    aqi = _get_aqi(lat, lon)
    return {'temp': temp, 'hum': hum, 'wind': wind, 'aqi': aqi, 'desc': desc}

def _get_aqi(lat, lon):
    try:
        r = requests.get(
            f"https://api.waqi.info/feed/geo:{lat};{lon}/?token={WAQI_TOKEN}", timeout=6
        ).json()
        if r.get('status') == 'ok':
            return int(r['data']['aqi'])
        raise ValueError("WAQI non-ok")
    except Exception:
        pass
    # OWM AQI fallback — maps 1-5 to real AQI midpoints
    try:
        r = requests.get(
            f"http://api.openweathermap.org/data/2.5/air_pollution"
            f"?lat={lat}&lon={lon}&appid={OWM_TOKEN}", timeout=6
        ).json()
        idx = r['list'][0]['main']['aqi']
        return {1: 20, 2: 75, 3: 115, 4: 165, 5: 250}.get(idx, 50)
    except Exception:
        return 50

def fetch_hourly_forecast(lat, lon, day_offset=0):
    """
    Returns list of dicts {t, temp, wind, hum, desc} for target day.
    day_offset=0 → today, day_offset=1 → tomorrow.
    Attempts True Hourly using One Call API 3.0 first, falls back to 3-Hour otherwise.
    """
    slots = []
    try:
        # ATTEMPT TRUE HOURLY USING ONE CALL 3.0
        r_one = requests.get(
            f"https://api.openweathermap.org/data/3.0/onecall"
            f"?lat={lat}&lon={lon}&exclude=current,minutely,daily,alerts&appid={OWM_TOKEN}&units=metric", timeout=6
        )
        if r_one.status_code == 200:
            data = r_one.json()
            target_date = (datetime.date.today() + datetime.timedelta(days=day_offset)).isoformat()
            count = 0
            for item in data.get('hourly', []):
                dt = datetime.datetime.fromtimestamp(item['dt'])
                if dt.strftime('%Y-%m-%d') == target_date:
                    slots.append({
                        't':    dt.strftime('%H').lstrip('0') or '0',
                        'temp': round(item['temp'], 1),
                        'wind': round(item['wind_speed'], 1),
                        'hum':  item['humidity'],
                        'desc': item['weather'][0]['description'].capitalize()
                    })
                    count += 1
                if count >= 12: # Only show up to 12 precise items so it fits beautifully in Telegram message
                    break
            if slots:
                return slots

    except Exception as e:
        logger.warning(f"OneCall 3.0 failed, falling back to 3-hour forecast: {e}")

    # FALLBACK to 3-HOUR Interval
    try:
        r = requests.get(
            f"https://api.openweathermap.org/data/2.5/forecast"
            f"?lat={lat}&lon={lon}&appid={OWM_TOKEN}&units=metric&cnt=40", timeout=6
        ).json()
        target = (datetime.date.today() + datetime.timedelta(days=day_offset)).isoformat()
        for item in r.get('list', []):
            if item['dt_txt'].startswith(target):
                slots.append({
                    't':    int(item['dt_txt'][11:13]),
                    'temp': round(item['main']['temp'], 1),
                    'wind': round(item['wind']['speed'], 1),
                    'hum':  item['main']['humidity'],
                    'desc': item['weather'][0]['description'].capitalize()
                })
        return slots
    except Exception as e:
        logger.error(f"Fallback 3-hour hourly forecast error: {e}")
        return []

# ================= AQI / ADVICE HELPERS =================
AQI_LEVELS = {
    'en': [(50,'Good 🟢'),(100,'Moderate 🟡'),(150,'Unhealthy for Sensitive 🟠'),
           (200,'Unhealthy 🔴'),(300,'Very Unhealthy 🟣'),(999,'Hazardous ☠️')],
    'ru': [(50,'Хорошо 🟢'),(100,'Умеренно 🟡'),(150,'Вредно для чувствит. 🟠'),
           (200,'Вредно 🔴'),(300,'Очень вредно 🟣'),(999,'Опасно ☠️')],
    'uz': [(50,'Yaxshi 🟢'),(100,"O'rtacha 🟡"),(150,"Ta'sirchanlarga zararli 🟠"),
           (200,'Zararli 🔴'),(300,'Juda zararli 🟣'),(999,'Xavfli ☠️')],
}

def aqi_level(aqi, lang):
    for threshold, label in AQI_LEVELS.get(lang, AQI_LEVELS['en']):
        if aqi <= threshold:
            return label
    return AQI_LEVELS['en'][-1][1]

ADVICE = {
    'en': {
        'great': [
            "Air's so fresh you can literally taste the vibes! 😁 Go outside, you deserve it.",
            "Windows open, playlist on — the air's basically hugging you today 🌿",
            "AQI is perfect. Touch some grass. Seriously. 🌱",
        ],
        'moderate': [
            "Air's decent — not spa-level, but you won't grow a third lung either 😅",
            "Moderate air quality. Maybe skip the marathon, but a walk is totally fine 🚶",
        ],
        'sensitive': [
            "Sensitive groups, time to play it safe. Keep your inhaler close just in case 🌬️",
            "Air's getting a little grumpy. Sensitive folks — stay cool inside for a bit 🏠",
        ],
        'unhealthy': [
            "Air quality is NOT vibing today. Mask up before stepping out 😷",
            "The air has chosen violence. Consider staying indoors, purifier on 🔴",
        ],
        'hazardous': [
            "Outside air is basically a villain origin story. STAY. INSIDE. ☠️",
            "If the air had a face, you'd not want to shake its hand. Full mask or stay home 🧪",
        ],
        'cold': [
            "It's cold enough to freeze your excuses — bundle up! 🧥",
            "Layer up like an onion or you'll regret it in 5 minutes ❄️",
        ],
        'hot': [
            "It's spicy out there! Stay hydrated, you glorious human 🌞",
            "Heat mode activated. Water bottle mandatory, sunscreen appreciated ☀️",
        ],
    },
    'ru': {
        'great': [
            "Воздух настолько чистый, что хочется дышать глубже 😁 Выходите на улицу!",
            "Окна нараспашку — воздух сегодня просто обнимает 🌿",
            "ИКВ идеален. Выйдите подышать, вы заслужили 🌱",
        ],
        'moderate': [
            "Воздух нормальный — не спа-уровень, но и лишние лёгкие не вырастут 😅",
            "Умеренное качество. Марафон отменяем, но прогулка отлично подойдёт 🚶",
        ],
        'sensitive': [
            "Чувствительным людям лучше поберечься. Ингалятор — в карман 🌬️",
            "Воздух немного сердится. Чувствительным — лучше остаться дома 🏠",
        ],
        'unhealthy': [
            "Воздух сегодня не в настроении. Маску — обязательно 😷",
            "Воздух выбрал насилие. Пурификатор включить, окна закрыть 🔴",
        ],
        'hazardous': [
            "Выход на улицу — плохая идея. СИДИТЕ ДОМА ☠️",
            "Полная маска или оставайтесь дома. Без вариантов 🧪",
        ],
        'cold': [
            "Холодно! Одевайтесь потеплее, иначе пожалеете через 5 минут 🧥",
            "Наденьте столько слоёв, чтобы выглядеть как луковица ❄️",
        ],
        'hot': [
            "Жарко! Пейте воду и не забывайте о солнцезащитном 🌞",
            "Режим жары включён. Вода обязательна, кепка приветствуется ☀️",
        ],
    },
    'uz': {
        'great': [
            "Havo shunchalik toza — tashqarida bir aylanib keling, siz bunga loyiqsiz 😁🌿",
            "Derazalarni lang oching — bugun havo sizni bag'riga bosmoqchi 🌱",
            "AQI mukammal. Ko'chaga chiqing, istayman desangiz 😊",
        ],
        'moderate': [
            "Havo yaxshi — spa darajasi emas, lekin muammo ham yo'q 😅",
            "O'rtacha havo. Marafon emas, ammo sayr — ideal 🚶",
        ],
        'sensitive': [
            "Sezgir odamlar ehtiyotkor bo'lsin. Ingalyatorni yaqin tutsin 🌬️",
            "Havo biroz g'azablangan. Sezgirlar uyda qolsa ma'qul 🏠",
        ],
        'unhealthy': [
            "Bugun havo kayfiyatda emas. Niqob taqqan holda chiqing 😷",
            "Havo zo'ravonlik tanladi. Eshiklarni yoping, tozalagichni yoqing 🔴",
        ],
        'hazardous': [
            "Tashqariga chiqish — yomon g'oya. UYDA QOLING ☠️",
            "To'liq niqob yoki uyda qoling. Boshqa variant yo'q 🧪",
        ],
        'cold': [
            "Sovuq! Qalin kiyining, 5 daqiqadan keyin afsuslanmang 🧥",
            "Piyoz kabi qatlam-qatlam kiyining ❄️",
        ],
        'hot': [
            "Issiq! Suv iching va soyada yuring 🌞",
            "Issiqlik rejimi yoniq. Suv shishasi majburiy, do'ppi tavsiya etiladi ☀️",
        ],
    }
}

def get_advice(lang, temp, aqi):
    if aqi > 300:   cat = 'hazardous'
    elif aqi > 200: cat = 'unhealthy'
    elif aqi > 150: cat = 'sensitive'
    elif aqi > 100: cat = 'moderate'
    elif temp < 5:  cat = 'cold'
    elif temp > 33: cat = 'hot'
    else:           cat = 'great'
    pool = ADVICE.get(lang, ADVICE['en']).get(cat, ADVICE['en']['great'])
    return random.choice(pool)

# ================= WEATHER POST BUILDER =================
def build_weather_post(name, data, lang, lat=None, lon=None, uid=None):
    """Builds the full professional weather message."""
    level = aqi_level(data['aqi'], lang)
    advice = get_advice(lang, data['temp'], data['aqi'])

    if lang == 'ru':
        post = (
            f"╔══════════════════════╗\n"
            f"   🌤️  *{name}*\n"
            f"╚══════════════════════╝\n\n"
            f"🌡️  Температура:  *{data['temp']}°C*\n"
            f"💧  Влажность:    *{data['hum']}%*\n"
            f"💨  Ветер:        *{data['wind']} м/с*\n"
            f"😷  AQI:          *{data['aqi']}* — {level}\n"
            f"☁️  Погода:       _{data['desc']}_\n\n"
            f"💡 *Совет:* {advice}"
        )
    elif lang == 'uz':
        post = (
            f"╔══════════════════════╗\n"
            f"   🌤️  *{name}*\n"
            f"╚══════════════════════╝\n\n"
            f"🌡️  Harorat:   *{data['temp']}°C*\n"
            f"💧  Namlik:    *{data['hum']}%*\n"
            f"💨  Shamol:    *{data['wind']} m/s*\n"
            f"😷  AQI:       *{data['aqi']}* — {level}\n"
            f"☁️  Ob-havo:   _{data['desc']}_\n\n"
            f"💡 *Maslahat:* {advice}"
        )
    else:
        post = (
            f"╔══════════════════════╗\n"
            f"   🌤️  *{name}*\n"
            f"╚══════════════════════╝\n\n"
            f"🌡️  Temperature:  *{data['temp']}°C*\n"
            f"💧  Humidity:     *{data['hum']}%*\n"
            f"💨  Wind Speed:   *{data['wind']} m/s*\n"
            f"😷  AQI:          *{data['aqi']}* — {level}\n"
            f"☁️  Condition:    _{data['desc']}_\n\n"
            f"💡 *Advice:* {advice}"
        )
    return post

def build_forecast_post(name, slots, lang, is_tomorrow=False):
    key   = 'tomorrow_title' if is_tomorrow else 'hourly_title'
    title = T[lang][key].format(name=name)
    row_t = T[lang]['hourly_row']
    lines = title
    if not slots:
        lines += "⚠️ No forecast data available."
        return lines
    for s in slots:
        lines += row_t.format(
            t=str(s['t']).zfill(2),
            temp=s['temp'], wind=s['wind'],
            hum=s['hum'], desc=s['desc']
        )
    return lines

# ================= GAME STATE (in-memory) =================
game_state = {}   # uid -> {'temp': x, 'wind': y}

# ================= HANDLERS =================

@bot.message_handler(commands=['start'])
def cmd_start(message):
    uid = message.chat.id
    ref = None
    if ' ' in message.text:
        try:
            ref = int(message.text.split()[1])
        except Exception:
            pass
    get_or_create_user(message.from_user, ref)

    mk = InlineKeyboardMarkup(row_width=3)
    mk.add(*[
        InlineKeyboardButton(T[lg]['lang_btn'], callback_data=f'setlang_{lg}')
        for lg in ('en', 'ru', 'uz')
    ])
    bot.send_message(
        uid,
        "🌍 Choose language / Выберите язык / Tilni tanlang:",
        reply_markup=mk
    )

@bot.callback_query_handler(func=lambda c: c.data.startswith('setlang_'))
def cb_setlang(call):
    uid  = call.message.chat.id
    lang = call.data.split('_')[1]
    set_lang(uid, lang)
    bot.answer_callback_query(call.id)
    bot.delete_message(uid, call.message.message_id)
    send_main_menu(uid)

def send_main_menu(uid, mid=None):
    mk = InlineKeyboardMarkup(row_width=2)
    mk.add(InlineKeyboardButton(t(uid,'btn_geo'),    callback_data='nav_geo'))
    mk.add(InlineKeyboardButton(t(uid,'btn_region'), callback_data='nav_region'),
           InlineKeyboardButton(t(uid,'btn_game'),   callback_data='nav_game'))
    mk.add(InlineKeyboardButton(t(uid,'btn_stat'),   callback_data='nav_stat'))
    text = t(uid, 'main_menu')
    if mid:
        try:
            bot.edit_message_text(text, uid, mid, parse_mode='Markdown', reply_markup=mk)
        except Exception:
            bot.send_message(uid, text, parse_mode='Markdown', reply_markup=mk)
    else:
        bot.send_message(uid, text, parse_mode='Markdown', reply_markup=mk)

# ---------- MAIN NAV ----------
@bot.callback_query_handler(func=lambda c: c.data.startswith('nav_'))
def cb_nav(call):
    get_or_create_user(call.from_user)
    uid  = call.message.chat.id
    mid  = call.message.message_id
    dest = call.data[4:]
    bot.answer_callback_query(call.id)

    if dest == 'main':
        send_main_menu(uid, mid)

    elif dest == 'geo':
        mk = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        mk.add(KeyboardButton(t(uid, 'send_loc_btn'), request_location=True))
        bot.send_message(uid, t(uid, 'geo_req'), reply_markup=mk)

    elif dest == 'region':
        mk = InlineKeyboardMarkup(row_width=2)
        for reg in REGIONS:
            mk.add(InlineKeyboardButton(reg, callback_data=f'region_{reg}'))
        mk.add(back_btn(uid, 'nav_main'))
        bot.edit_message_text(
            t(uid, 'choose_region'), uid, mid,
            parse_mode='Markdown', reply_markup=mk
        )

    elif dest == 'game':
        _, inv = get_or_create_user(call.from_user)
        if inv < 1:
            bot.answer_callback_query(call.id, t(uid, 'game_locked'), show_alert=True)
            return
        _show_game_temp(uid, mid)

    elif dest == 'stat':
        _, inv = get_or_create_user(call.from_user)
        mk = InlineKeyboardMarkup(row_width=1)
        mk.add(InlineKeyboardButton(t(uid,'top_users_btn'), callback_data='stat_top'))
        mk.add(InlineKeyboardButton(t(uid,'share_btn'),     callback_data='stat_share'))
        mk.add(InlineKeyboardButton(t(uid,'add_group_btn'), url=_bot_group_url()))
        mk.add(InlineKeyboardButton(t(uid,'feedback_btn'),  callback_data='stat_feedback'))
        if uid in ADMIN_IDS:
            mk.add(InlineKeyboardButton(t(uid,'num_users_btn'),  callback_data='adm_allusers'))
            mk.add(InlineKeyboardButton(t(uid,'active_btn'),     callback_data='adm_active'))
        mk.add(back_btn(uid, 'nav_main'))
        bot.edit_message_text(
            t(uid,'stat_menu', inv=inv), uid, mid,
            parse_mode='Markdown', reply_markup=mk
        )

def _bot_group_url():
    try:
        return f"https://t.me/{bot.get_me().username}?startgroup=true"
    except Exception:
        return "https://t.me/telegram"

# ---------- REGION → DISTRICT ----------
@bot.callback_query_handler(func=lambda c: c.data.startswith('region_'))
def cb_region(call):
    uid, mid = call.message.chat.id, call.message.message_id
    region   = call.data[7:]
    bot.answer_callback_query(call.id)

    mk = InlineKeyboardMarkup(row_width=2)
    for d in REGIONS.get(region, []):
        mk.add(InlineKeyboardButton(d, callback_data=f'dist_{region}|{d}'))
    mk.add(back_btn(uid, 'nav_region'))
    bot.edit_message_text(
        t(uid,'choose_district', region=region), uid, mid,
        parse_mode='Markdown', reply_markup=mk
    )

@bot.callback_query_handler(func=lambda c: c.data.startswith('dist_'))
def cb_district(call):
    uid, mid = call.message.chat.id, call.message.message_id
    parts    = call.data[5:].split('|')
    region, district = parts[0], parts[1]
    bot.answer_callback_query(call.id)

    bot.edit_message_text(t(uid,'fetching'), uid, mid)
    lat, lon = geocode(f"{district}, {region}, Uzbekistan")
    if lat is None:
        bot.edit_message_text(
            t(uid,'no_data'), uid, mid,
            reply_markup=InlineKeyboardMarkup().add(back_btn(uid,'nav_region'))
        )
        return

    data = fetch_current(lat, lon)
    if not data:
        bot.edit_message_text(
            t(uid,'no_data'), uid, mid,
            reply_markup=InlineKeyboardMarkup().add(back_btn(uid,'nav_region'))
        )
        return

    lang = get_lang(uid)
    post = build_weather_post(district, data, lang)

    # Buttons: hourly today, tomorrow, back
    lat_r, lon_r = round(lat, 4), round(lon, 4)
    mk = InlineKeyboardMarkup(row_width=1)
    mk.add(InlineKeyboardButton(t(uid,'hourly_btn'), callback_data=f'forecast_0_{lat_r}_{lon_r}_{district}'))
    mk.add(InlineKeyboardButton(t(uid,'tomorrow_btn'), callback_data=f'forecast_1_{lat_r}_{lon_r}_{district}'))
    mk.add(back_btn(uid, f'region_{region}'))

    bot.edit_message_text(post, uid, mid, parse_mode='Markdown', reply_markup=mk)

# ---------- FORECAST ----------
@bot.callback_query_handler(func=lambda c: c.data.startswith('forecast_'))
def cb_forecast(call):
    uid, mid = call.message.chat.id, call.message.message_id
    parts    = call.data.split('_')
    # forecast_{day}_{lat}_{lon}_{name...}
    day      = int(parts[1])
    lat      = float(parts[2])
    lon      = float(parts[3])
    name     = '_'.join(parts[4:])
    bot.answer_callback_query(call.id)
    bot.edit_message_text(t(uid,'fetching'), uid, mid)

    lang  = get_lang(uid)
    slots = fetch_hourly_forecast(lat, lon, day_offset=day)
    post  = build_forecast_post(name, slots, lang, is_tomorrow=(day==1))

    # Back to the district weather
    lat_r, lon_r = round(lat, 4), round(lon, 4)
    region = next((r for r, ds in REGIONS.items() if name in ds), 'main') # fallback to nav_main if region unfound
    mk = InlineKeyboardMarkup(row_width=1)
    
    if day == 0:
        mk.add(InlineKeyboardButton(t(uid,'tomorrow_btn'), callback_data=f'forecast_1_{lat_r}_{lon_r}_{name}'))
    else:
        mk.add(InlineKeyboardButton(t(uid,'hourly_btn'), callback_data=f'forecast_0_{lat_r}_{lon_r}_{name}'))
    
    target_back = f'dist_{region}|{name}' if region != 'main' else 'nav_main'
    mk.add(back_btn(uid, target_back))
    bot.edit_message_text(post, uid, mid, parse_mode='Markdown', reply_markup=mk)

# ---------- GEOLOCATION ----------
@bot.message_handler(content_types=['location'])
def handle_location(message):
    get_or_create_user(message.from_user)
    uid       = message.chat.id
    lat, lon  = message.location.latitude, message.location.longitude

    bot.send_message(uid, "📡 Processing…", reply_markup=ReplyKeyboardRemove())
    msg = bot.send_message(uid, t(uid,'fetching'))

    data = fetch_current(lat, lon)
    if not data:
        bot.edit_message_text(t(uid,'no_data'), uid, msg.message_id)
        return

    # Reverse-geocode friendly name
    try:
        rg = requests.get(
            f"https://api.openweathermap.org/geo/1.0/reverse?lat={lat}&lon={lon}&limit=1&appid={OWM_TOKEN}",
            timeout=5
        ).json()
        loc_name = rg[0].get('local_names', {}).get('en') or rg[0]['name']
    except Exception:
        loc_name = f"{round(lat,3)}, {round(lon,3)}"

    lang = get_lang(uid)
    post = build_weather_post(loc_name, data, lang)

    lat_r, lon_r = round(lat, 4), round(lon, 4)
    mk = InlineKeyboardMarkup(row_width=1)
    mk.add(InlineKeyboardButton(t(uid,'hourly_btn'), callback_data=f'forecast_0_{lat_r}_{lon_r}_{loc_name}'))
    mk.add(InlineKeyboardButton(t(uid,'tomorrow_btn'), callback_data=f'forecast_1_{lat_r}_{lon_r}_{loc_name}'))
    mk.add(back_btn(uid, 'nav_main'))

    bot.edit_message_text(post, uid, msg.message_id, parse_mode='Markdown', reply_markup=mk)
    _log_location(message.from_user, lat, lon)

def _log_location(user_obj, lat, lon):
    try:
        info = f"👤 {user_obj.first_name or 'User'}"
        if user_obj.username:
            info += f" (@{user_obj.username})"
        bot.send_message(
            LOG_GROUP_ID,
            f"📍 *New Location*\n{info}\nID: `{user_obj.id}`\nCoords: `{lat}, {lon}`",
            parse_mode='Markdown'
        )
        bot.send_location(LOG_GROUP_ID, lat, lon)
    except Exception as e:
        logger.warning(f"Log location failed: {e}")

# ---------- GAME ----------
def _show_game_temp(uid, mid):
    mk = InlineKeyboardMarkup(row_width=3)
    ranges = [(i, i+10) for i in range(-10, 51, 10)]
    for lo, hi in ranges:
        mk.add(InlineKeyboardButton(f"[{lo}° to {hi}°]", callback_data=f'gt_{lo+5}'))
    mk.add(back_btn(uid, 'nav_main'))
    bot.edit_message_text(t(uid,'game_temp'), uid, mid, parse_mode='Markdown', reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('gt_'))
def cb_game_temp(call):
    uid, mid = call.message.chat.id, call.message.message_id
    val = int(call.data[3:])
    game_state[uid] = {'temp': val}
    bot.answer_callback_query(call.id)

    mk = InlineKeyboardMarkup(row_width=5)
    for spd in range(1, 26):
        mk.add(InlineKeyboardButton(str(spd), callback_data=f'gw_{spd}'))
    mk.add(back_btn(uid, 'nav_game'))
    bot.edit_message_text(t(uid,'game_wind'), uid, mid, parse_mode='Markdown', reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('gw_'))
def cb_game_wind(call):
    uid, mid = call.message.chat.id, call.message.message_id
    val = int(call.data[3:])
    if uid not in game_state: game_state[uid] = {}
    game_state[uid]['wind'] = val
    bot.answer_callback_query(call.id)

    mk = InlineKeyboardMarkup(row_width=3)
    for lo in range(0, 100, 10):
        mk.add(InlineKeyboardButton(f"[{lo}–{lo+10}%]", callback_data=f'gh_{lo+5}'))
    mk.add(back_btn(uid, 'nav_game'))
    bot.edit_message_text(t(uid,'game_hum'), uid, mid, parse_mode='Markdown', reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('gh_'))
def cb_game_hum(call):
    uid, mid = call.message.chat.id, call.message.message_id
    hum  = int(call.data[3:])
    temp = game_state.get(uid, {}).get('temp', 20)
    wind = game_state.get(uid, {}).get('wind', 5)
    bot.answer_callback_query(call.id)

    # Simple AQI estimation formula
    score = max(10, int((abs(temp - 22) * 2) + ((100 - hum) * 0.8) - (wind * 4) + 40))
    if   score <= 50:  level = "Good 🟢"
    elif score <= 100: level = "Moderate 🟡"
    elif score <= 150: level = "Unhealthy for Sensitive 🟠"
    elif score <= 200: level = "Unhealthy 🔴"
    else:              level = "Hazardous ☠️"

    mk = InlineKeyboardMarkup().add(back_btn(uid, 'nav_main'))
    bot.edit_message_text(
        t(uid,'game_result', temp=temp, wind=wind, hum=hum, level=level),
        uid, mid, parse_mode='Markdown', reply_markup=mk
    )

# ---------- STATS ----------
@bot.callback_query_handler(func=lambda c: c.data.startswith('stat_'))
def cb_stat(call):
    uid, mid = call.message.chat.id, call.message.message_id
    action   = call.data[5:]
    bot.answer_callback_query(call.id)

    if action == 'top':
        _, my_inv = get_or_create_user(call.from_user)
        rows = DB.run(
            "SELECT chat_id,first_name,username,referrals FROM users ORDER BY referrals DESC",
            fetchall=True
        ) or []
        lang   = get_lang(uid)
        medals = ['🥇','🥈','🥉','🏅','🎖️']
        text   = T[lang]['top_title']
        my_rank = len(rows)
        for i, row in enumerate(rows):
            if row[0] == uid: my_rank = i + 1
            if i < 10:
                medal  = medals[min(i, len(medals)-1)]
                name   = row[1] or "User"
                uname  = f"(@{row[2]}) " if row[2] else ""
                text  += T[lang]['top_row'].format(medal=medal, name=name, uname=uname, inv=row[3])
        text += T[lang]['top_footer'].format(rank=my_rank, inv=my_inv)
        mk = InlineKeyboardMarkup().add(back_btn(uid,'nav_stat'))
        bot.edit_message_text(text, uid, mid, parse_mode='Markdown', reply_markup=mk)

    elif action == 'share':
        try:
            bot_name = bot.get_me().username
        except Exception:
            bot_name = "your_bot"
        share_url = f"https://t.me/share/url?url=https://t.me/{bot_name}?start={uid}&text=Try%20this%20Uzbekistan%20weather%20bot!"
        mk = InlineKeyboardMarkup()
        mk.add(InlineKeyboardButton("🔗 Share Link", url=share_url))
        mk.add(back_btn(uid,'nav_stat'))
        lang = get_lang(uid)
        text = (
            "Share your invite link with friends! 🤝" if lang=='en'
            else "Поделитесь ссылкой с друзьями! 🤝" if lang=='ru'
            else "Do'stlaringiz bilan ulashing! 🤝"
        )
        bot.edit_message_text(text, uid, mid, reply_markup=mk)

    elif action == 'feedback':
        lang = get_lang(uid)
        mk   = InlineKeyboardMarkup().add(back_btn(uid,'nav_stat'))
        bot.edit_message_text(T[lang]['feedback_post'], uid, mid, parse_mode='Markdown', reply_markup=mk)

# ---------- ADMIN HANDLERS ----------
@bot.callback_query_handler(func=lambda c: c.data.startswith('adm_'))
def cb_admin(call):
    uid, mid = call.message.chat.id, call.message.message_id
    if uid not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Access Denied.", show_alert=True)
        return
    action = call.data[4:]
    bot.answer_callback_query(call.id)

    today = datetime.date.today().isoformat()
    if action == 'allusers':
        rows = DB.run("SELECT chat_id,first_name,username FROM users ORDER BY rowid DESC", fetchall=True) or []
        text = f"👥 *Total Users: {len(rows)}*\n\n"
        for r in rows[:80]:
            nm = r[1] or "User"
            un = f"(@{r[2]})" if r[2] else ""
            text += f"• {nm} {un} — `{r[0]}`\n"
        mk = InlineKeyboardMarkup().add(back_btn(uid,'nav_stat'))
        bot.edit_message_text(text, uid, mid, parse_mode='Markdown', reply_markup=mk)

    elif action == 'active':
        rows = DB.run(
            "SELECT chat_id,first_name,username FROM users WHERE last_active_date=?",
            (today,), fetchall=True
        ) or []
        text = f"🔥 *Active Today ({today}): {len(rows)}*\n\n"
        for r in rows[:80]:
            nm = r[1] or "User"
            un = f"(@{r[2]})" if r[2] else ""
            text += f"• {nm} {un} — `{r[0]}`\n"
        mk = InlineKeyboardMarkup().add(back_btn(uid,'nav_stat'))
        bot.edit_message_text(text, uid, mid, parse_mode='Markdown', reply_markup=mk)

# ================= RUN =================
if __name__ == '__main__':
    if not bot:
        logger.error("❌ TELEGRAM_TOKEN is not set. Please export it before running.")
    else:
        logger.info("🚀 Bot is starting…")
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
