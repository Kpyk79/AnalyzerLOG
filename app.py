import os
import io
import csv
import json
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template, Response
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

FEET_TO_M = 0.3048
MPH_TO_MS = 0.44704
MPH_TO_KMH = 1.60934


def parse_val(v):
    try:
        return float(str(v).strip())
    except Exception:
        return None


def detect_separator(first_line):
    """Return '\t', ',' or ';' based on which delimiter dominates the header line."""
    tabs   = first_line.count('\t')
    commas = first_line.count(',')
    semis  = first_line.count(';')
    if tabs > commas and tabs > semis:
        return '\t'
    return ',' if commas > semis else ';'


def normalize_datetime(dt_str):
    """Convert any recognized datetime format to DD.MM.YYYY H:MM."""
    dt_str = dt_str.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M"):
        try:
            dt = datetime.strptime(dt_str, fmt)
            # Format: day no-padded, month zero-padded, hour no-padded, minute zero-padded
            return f"{dt.day}.{dt.month:02d}.{dt.year} {dt.hour}:{dt.minute:02d}"
        except ValueError:
            pass
    return dt_str  # return unchanged if not recognized


def normalize_csv(text):
    """
    Convert comma-separated Airdata CSV (format 1) to semicolon-separated
    format 2.  Uses csv.reader so quoted fields (e.g. message) are parsed
    correctly and stored without surrounding quotes in the output — exactly
    matching the structure of native format-2 files.
    Also converts datetime(utc) to DD.MM.YYYY H:MM.
    Returns (normalized_text, was_converted).
    """
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return text, False

    sep = detect_separator(lines[0])
    if sep != ',':
        return text, False  # tab or semicolon — no conversion needed

    reader = csv.reader(io.StringIO(text))          # handles quoted fields
    all_rows = list(reader)
    if not all_rows:
        return text, False

    headers = [h.strip() for h in all_rows[0]]
    dt_col  = next((i for i, h in enumerate(headers)
                    if h == 'datetime(utc)'), None)

    out_lines = [';'.join(headers)]
    for row in all_rows[1:]:
        row = list(row)
        if dt_col is not None and dt_col < len(row):
            row[dt_col] = normalize_datetime(row[dt_col])
        out_lines.append(';'.join(row))

    return '\n'.join(out_lines), True


def parse_csv(text):
    """Parse tab-, comma- or semicolon-separated Airdata CSV into (headers, rows)."""
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return None, None

    sep = detect_separator(lines[0])

    if sep == ';':
        # Already normalised — simple split
        headers = [h.strip() for h in lines[0].split(';')]
        rows = []
        for line in lines[1:]:
            cols = line.split(';')
            row = {}
            for i, h in enumerate(headers):
                row[h] = cols[i].strip() if i < len(cols) else ""
            rows.append(row)
        return headers, rows
    else:
        # tab or comma: use csv module, then normalise datetime on the fly
        reader = csv.reader(io.StringIO(text), delimiter=sep)
        all_rows = list(reader)
        if not all_rows:
            return None, None
        headers = [h.strip() for h in all_rows[0]]
        dt_col = next((i for i, h in enumerate(headers) if h == 'datetime(utc)'), None)
        rows = []
        for r in all_rows[1:]:
            row = {}
            for i, h in enumerate(headers):
                val = r[i].strip() if i < len(r) else ""
                if i == dt_col:
                    val = normalize_datetime(val)
                row[h] = val
            rows.append(row)
        return headers, rows


def gv(row, *keys):
    for k in keys:
        if k in row:
            v = parse_val(row[k])
            if v is not None:
                return v
    return 0.0


def extract_metrics(rows):
    t, h, spd_ms, spd_kmh, dist, sats, bat = [], [], [], [], [], [], []
    pitch, roll, hdg, lat, lon, z_ms, volt = [], [], [], [], [], [], []
    states, msgs, dtimes = [], [], []

    for r in rows:
        t_ms = gv(r, "time(millisecond)")
        t.append(t_ms / 1000.0)
        h.append(gv(r, "height_above_takeoff(feet)") * FEET_TO_M)
        s_mph = gv(r, "speed(mph)")
        spd_ms.append(s_mph * MPH_TO_MS)
        spd_kmh.append(s_mph * MPH_TO_KMH)
        dist.append(gv(r, "distance(feet)") * FEET_TO_M)
        sats.append(int(gv(r, "satellites")))
        bat.append(gv(r, "battery_percent"))
        pitch.append(gv(r, " pitch(degrees)", "pitch(degrees)"))
        roll.append(gv(r, " roll(degrees)", "roll(degrees)"))
        hdg.append(gv(r, " compass_heading(degrees)", "compass_heading(degrees)"))
        la = parse_val(r.get("latitude", ""))
        lo = parse_val(r.get("longitude", ""))
        lat.append(la if la is not None else 0.0)
        lon.append(lo if lo is not None else 0.0)
        z_mph = gv(r, " zSpeed(mph)", "zSpeed(mph)")
        z_ms.append(z_mph * MPH_TO_MS)
        volt.append(gv(r, "voltage(v)"))
        states.append(r.get("flycState", ""))
        msgs.append(r.get("message", ""))
        dtimes.append(r.get("datetime(utc)", ""))

    return dict(t=t, h=h, spd_ms=spd_ms, spd_kmh=spd_kmh, dist=dist,
                sats=sats, bat=bat, pitch=pitch, roll=roll, hdg=hdg,
                lat=lat, lon=lon, z_ms=z_ms, volt=volt,
                states=states, msgs=msgs, dtimes=dtimes)


def local_time(dt_str, offset_h=2):
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M"):
        try:
            dt = datetime.strptime(dt_str.strip(), fmt)
            return (dt + timedelta(hours=offset_h)).strftime("%H:%M:%S")
        except Exception:
            pass
    return dt_str


def local_date(dt_str):
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M"):
        try:
            dt = datetime.strptime(dt_str.strip(), fmt)
            return dt.strftime("%d.%m.%Y")
        except Exception:
            pass
    return dt_str


def sec_to_local(start_dt_str, seconds, offset_h=2):
    """Convert flight seconds offset into local HH:MM time string."""
    if seconds is None:
        return "—"
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M"):
        try:
            dt = datetime.strptime(start_dt_str.strip(), fmt)
            event_dt = dt + timedelta(hours=offset_h, seconds=float(seconds))
            return event_dt.strftime("%H:%M")
        except Exception:
            pass
    return "—"


def build_summary(m):
    t = m["t"]
    if not t:
        return {}

    duration = max(t)
    max_h = max(m["h"])
    max_spd_ms = max(m["spd_ms"])
    max_spd_kmh = max(m["spd_kmh"])
    max_dist = max(m["dist"])
    start_dt = m["dtimes"][0] if m["dtimes"] else ""
    end_dt = m["dtimes"][-1] if m["dtimes"] else ""

    # Jamming: satellites drop to 0 after takeoff
    jamming_start = None
    jamming_end = None
    for i, (ts, s) in enumerate(zip(t, m["sats"])):
        if ts > 5 and s == 0 and jamming_start is None:
            jamming_start = ts
        if jamming_start and s > 0 and ts > jamming_start and jamming_end is None:
            jamming_end = ts

    # Spoofing: lat/lon near (0, 0)
    spoof_time = None
    for ts, la, lo in zip(t, m["lat"], m["lon"]):
        if ts > 10 and abs(la) < 1.0 and abs(lo) < 1.0:
            spoof_time = ts
            break

    # ATTI transitions
    atti_times = []
    for ts, st in zip(t, m["states"]):
        if "ATTI" in st.upper():
            if not atti_times or ts - atti_times[-1] > 5:
                atti_times.append(ts)

    max_descent = abs(min(m["z_ms"])) if m["z_ms"] else 0

    # Unique key messages (skip empty / whitespace)
    seen = set()
    key_messages = []
    for ts, msg in zip(t, m["msgs"]):
        if msg and msg.strip() and msg not in seen:
            seen.add(msg)
            key_messages.append({"t": round(ts, 1), "msg": msg})

    all_msgs_lc = " ".join(m["msgs"]).lower()
    is_crash = any(w in all_msgs_lc for w in ["shock", "crash", "exiting gps"])

    min_bat = min((b for b in m["bat"] if b > 0), default=0)
    final_height = m["h"][-1] if m["h"] else 0

    # Max satellites during spoofing window (fake high count)
    spoof_sats = []
    if spoof_time:
        for ts, s in zip(t, m["sats"]):
            if ts >= spoof_time:
                spoof_sats.append(s)
                if len(spoof_sats) > 50:
                    break
    max_spoof_sats = max(spoof_sats) if spoof_sats else 0

    j_s = round(jamming_start, 1) if jamming_start else None
    sp_s = round(spoof_time, 1) if spoof_time else None

    return {
        "duration_s": round(duration, 1),
        "max_height_m": round(max_h, 1),
        "max_speed_ms": round(max_spd_ms, 1),
        "max_speed_kmh": round(max_spd_kmh, 1),
        "max_distance_m": round(max_dist, 1),
        "start_time_local": local_time(start_dt),
        "end_time_local": local_time(end_dt),
        "date": local_date(start_dt),
        "is_crash": is_crash,
        "jamming_start_s": j_s,
        "jamming_start_local": sec_to_local(start_dt, j_s),
        "jamming_end_s": round(jamming_end, 1) if jamming_end else None,
        "spoof_time_s": sp_s,
        "spoof_time_local": sec_to_local(start_dt, sp_s),
        "atti_times": [round(x, 1) for x in atti_times[:5]],
        "atti_first_local": sec_to_local(start_dt, atti_times[0]) if atti_times else None,
        "max_descent_ms": round(max_descent, 1),
        "final_height_m": round(final_height, 1),
        "min_battery_pct": round(min_bat, 1),
        "max_spoof_sats": max_spoof_sats,
        "key_messages": key_messages[:25],
        "total_rows": len(t),
        "start_dt_raw": start_dt,
    }


def build_prompt(summary, filename, mission_type="ВТРАТА",
                 firmware_1001="немає", antispoof_active=None):
    s = summary
    j = json.dumps(s, ensure_ascii=False, indent=2)

    msgs_block = "\n".join(
        f"  {m['t']}с: {m['msg']}" for m in s.get("key_messages", [])
    ) or "  (повідомлення відсутні)"

    has_jamming = s.get("jamming_start_s") is not None
    has_spoof   = s.get("spoof_time_s") is not None
    has_atti    = bool(s.get("atti_times"))

    mission_word     = "втрати" if mission_type == "ВТРАТА" else "пошкодження"
    mission_word_cap = "Втрата" if mission_type == "ВТРАТА" else "Пошкодження"
    section4_title   = ("ПРИЧИНИ ВТРАТИ БОРТУ (ДІАГНОСТИКА)" if mission_type == "ВТРАТА"
                        else "ПРИЧИНИ ПОШКОДЖЕННЯ БОРТУ (ДІАГНОСТИКА)")

    # ── Firmware / antispoof context ─────────────────────────────────────
    has_1001    = firmware_1001 == "є"
    activated   = has_1001 and antispoof_active == "активовано"
    not_activ   = has_1001 and antispoof_active == "не активовано"

    # П1 — jamming + ATTI, з урахуванням прошивки
    if has_1001 and activated:
        p1_firmware = (
            f"На борту БпЛА встановлена модифікована прошивка «1001» з активованим режимом "
            f"антиспуфінгу. Активація антиспуфінгу є примусовим відключенням навігаційного "
            f"стеку, що унеможливлює подальший спуфінг координат. Процес активації займає "
            f"5–15 секунд, протягом яких апарат може залишатися вразливим."
        )
    elif has_1001 and not_activ:
        p1_firmware = (
            f"На борту БпЛА встановлена модифікована прошивка «1001», однак режим "
            f"антиспуфінгу (примусове відключення навігаційного стеку) не був активований "
            f"до входу в зону дії засобів РЕБ. Тому система відпрацювала за штатними "
            f"алгоритмами виробника (DJI)."
        )
    else:
        p1_firmware = (
            f"На борту БпЛА не встановлена модифікована прошивка «1001», тому система "
            f"відпрацювала за штатними алгоритмами виробника (DJI), автоматично перевівши "
            f"апарат у режим ATTI (ручне керування без GPS-стабілізації) через втрату супутників."
        )

    # П6 — дії оператора
    if activated:
        p6_operator = (
            f"По даним з лог-файлів оператор активував режим антиспуфінгу прошивки «1001», "
            f"що призвело до примусового відключення навігаційного стеку. Активація займає "
            f"5–15 секунд, і якщо за цей час засоби РЕБ вже почали вплив, апарат міг "
            f"короткочасно залишатися вразливим. Подальший GNSS-спуфінг після активації "
            f"унеможливлюється апаратно."
        )
    elif not_activ:
        p6_operator = (
            f"По даним з лог-файлів оператор не активував режим антиспуфінгу прошивки «1001» "
            f"до входу в зону дії засобів РЕБ. Незважаючи на наявність захисного ПЗ, "
            f"відсутність своєчасної активації залишила навігаційний стек відкритим для "
            f"підміни координат. Перехід у режим ATTI відбувся автоматично за алгоритмом DJI "
            f"при втраті супутників, а не внаслідок дій пілота."
        )
    else:
        p6_operator = (
            f"По даним з лог-файлів дії оператора щодо управління БпЛА в частині переведення "
            f"його на ручний режим польоту (ATTI) зафіксовані не були. Перехід у режим ATTI "
            f"відбувся автоматично за алгоритмом DJI при втраті зв'язку з супутниками. "
            f"Оскільки оператор не зафіксував цей режим примусово на пульті керування, "
            f"при появі перших фальшивих сигналів спуфінгу система спробувала повернутися "
            f"до GPS-стабілізації, що і призвело до неконтрольованого ривка та подальшого "
            f"падіння на висоті {s.get('final_height_m', '—')} м."
        )

    # П7 — підсумок
    if activated:
        p7_conclusion = (
            f"Наявність прошивки «1001» та своєчасна активація режиму антиспуфінгу "
            f"свідчать про правильну тактичну підготовку оператора. Примусове відключення "
            f"навігаційного стеку унеможливлює подальший GNSS-спуфінг. Разом з тим, "
            f"часове вікно активації (5–15 с) є критичним — за цей інтервал засоби РЕБ "
            f"можуть здійснити первинний вплив на координати на дистанції "
            f"{s.get('max_distance_m', '—')} м від точки зльоту."
        )
    elif not_activ:
        p7_conclusion = (
            f"Незважаючи на наявність прошивки «1001», відсутність своєчасної активації "
            f"режиму антиспуфінгу залишила навігаційний стек вразливим до підміни координат. "
            f"Активація антиспуфінгу до входу в зону дії засобів РЕБ унеможливила б динамічну "
            f"підміну координат і, ймовірно, дозволила б зберегти апарат на дистанції "
            f"{s.get('max_distance_m', '—')} м від точки зльоту."
        )
    else:
        p7_conclusion = (
            f"Своєчасна реакція оператора та примусовий перехід у ручний режим польоту до "
            f"входу в зону дії спуфінгу могли б дозволити польотному контролеру ігнорувати "
            f"хибні дані. В умовах відсутності спеціалізованого ПЗ (як-от «1001»), що блокує "
            f"навігаційний стек на програмному рівні, апарат залишився вразливим до динамічної "
            f"підміни координат на дистанції {s.get('max_distance_m', '—')} м від точки зльоту."
        )

    # Кваліфікація дій пілота
    if activated:
        pilot_qual = "ВІРНІ"
        pilot_comment = (
            "Оператор своєчасно активував антиспуфінговий режим прошивки «1001», "
            "що є правильною тактичною дією в зоні дії засобів РЕБ."
        )
    elif not_activ:
        pilot_qual = "ТАКТИЧНА ПОМИЛКА"
        pilot_comment = (
            "Незважаючи на наявність прошивки «1001», оператор не активував режим "
            "антиспуфінгу до входу в зону дії засобів РЕБ. Наявність захисту без його "
            "активації не запобігає GNSS-спуфінгу."
        )
    else:
        pilot_qual = "ТАКТИЧНА ПОМИЛКА" if has_spoof or has_jamming else "ВІРНІ"
        pilot_comment = (
            "Оператор не перейшов у ручний режим ATTI вручну до входу в зону дії РЕБ. "
            "Відсутність прошивки «1001» залишала апарат повністю вразливим до спуфінгу."
            if has_spoof or has_jamming else
            "Відхилень у діях оператора не виявлено."
        )

    # ── Технічний висновок (7 абзаців без заголовків) ────────────────────
    technical_conclusion = f"""
ТЕХНІЧНИЙ ВИСНОВОК (ЗА ВСТАНОВЛЕНИМ ЗРАЗКОМ) — рівно 7 абзаців суцільним текстом, БЕЗ будь-яких заголовків чи нумерації абзаців у виводі:

Починаючи з {s.get('jamming_start_s', '—')} секунди польоту ({s.get('jamming_start_local', '—')}), спостерігалася критична деградація навігаційного сигналу через вплив засобів РЕБ. {p1_firmware}
{"[ЯКЩО немає jamming — адаптуй цей абзац до фактичних даних]" if not has_jamming else ""}

На {s.get('spoof_time_s', '—')} секунді ({s.get('spoof_time_local', '—')}) зафіксовано початок агресивного GNSS-спуфінгу, що призвело до миттєвої підміни координат на «нульові» (0,0). Це спричинило програмний шок польотного контролера, який намагався відновити позиціонування на висоті {s.get('max_height_m', '—')} м.
{"[ЯКЩО немає спуфінгу — опиши фактичну аномалію]" if not has_spoof else ""}

Враховуючи характер зміни параметрів, дані події ідентифікуються як результат інтелектуальної атаки на навігаційний стек. При отриманні фальшивих координат «Null Island» штатні алгоритми стабілізації DJI ініціювали максимальну потужність двигунів для компенсації уявного зміщення, що призвело до стрибка швидкості до {s.get('max_speed_ms', '—')} м/с ({s.get('max_speed_kmh', '—')} км/год).

В останніх кадрах телеметрії зафіксовано інтенсивний зовнішній вплив засобів радіоелектронної боротьби на внутрішні шини даних. Це підтверджується [вкажи конкретні повідомлення з key_messages] та імітацією високого статусу сигналу ({s.get('max_spoof_sats', '—')} супутників) при фактичному обнуленні координат. [ЯКЩО є shock-повідомлення]: Одночасно зафіксовано критичні «удари» в повітрі (Detected side/backward shock), що свідчить про механічне руйнування або позамежні вібрації конструкції через аномальний крутний момент двигунів.

Аномальна швидкість. Наприкінці логу зафіксовано стрибок горизонтальної швидкості до {s.get('max_speed_kmh', '—')} км/год. Це є класичним індикатором підміни координат, коли польотний контролер намагається стримати хибний вектор швидкості, спричинений миттєвим «переміщенням» у Гвінейську затоку (0,0).

{p6_operator}

{p7_conclusion}
"""

    # ── Контекст для ШІ про 1001 ─────────────────────────────────────────
    firmware_context = f"""
КОНТЕКСТ ПРОШИВКИ «1001»:
- Статус на борту: {"ВСТАНОВЛЕНА" if has_1001 else "НЕ ВСТАНОВЛЕНА"}
- Режим антиспуфінгу: {antispoof_active if has_1001 else "н/з (прошивки немає)"}

Визначення понять:
• Прошивка «1001» — модифікована прошивка DJI з вбудованим захистом від GNSS-спуфінгу.
• Антиспуфінг / ручний режим — примусове відключення навігаційного стеку GPS/GNSS на програмному рівні. Після активації польотний контролер повністю ігнорує будь-які GPS-координати, унеможливлюючи підміну позиції через спуфінг.
• Час активації антиспуфінгу: 5–15 секунд. Протягом цього вікна апарат залишається частково вразливим.
• Без активації антиспуфінгу прошивка «1001» НЕ захищає від спуфінгу — захист потребує ручної активації оператором.

Вплив на аналіз дій пілота:
{"- Оператор МАВ прошивку і АКТИВУВАВ антиспуфінг → кваліфікація ВІРНІ, але оцінити затримку активації відносно початку впливу РЕБ." if activated else ""}
{"- Оператор МАВ прошивку але НЕ АКТИВУВАВ → ТАКТИЧНА ПОМИЛКА: захист був доступний, але не використаний." if not_activ else ""}
{"- Прошивки НЕ БУЛО → аналізувати лише ручні дії пілота (перехід ATTI, реакція на джамінг)." if not has_1001 else ""}
"""

    return f"""Ти — військовий аналітик польотних даних БпЛА. Надай повний звіт ВИКЛЮЧНО українською мовою.

КРИТИЧНІ ПРАВИЛА:
- НІКОЛИ не вживай слова "ворожий", "противник" щодо РЕБ — пиши нейтрально "засоби РЕБ", "вплив РЕБ"
- Всі одиниці — метрична система (м, м/с, км/год)
- Час завжди у форматі UTC+2
- Тип місії: {mission_type} ({"знищення/неповернення борту" if mission_type == "ВТРАТА" else "пошкодження борту з можливістю відновлення"})
- У ТЕХНІЧНОМУ ВИСНОВКУ: виводь ТІЛЬКИ суцільний текст 7 абзаців — БЕЗ заголовків, нумерації, слів «Абзац», дужок із описами
- Замість [значення] підставляй реальні цифри з даних
{firmware_context}
Файл: {filename}
Дані польоту:
{j}

Повідомлення з логу:
{msgs_block}

Надай відповідь у ТОЧНО такому форматі:

===MESSENGER===
📊 Звіт щодо {mission_word} БпЛА ({s.get('date', '')}) — Місія {s.get('start_time_local', '')}
Причина: [одне речення — головна причина {mission_word}]
Деталі: Політ {s.get('duration_s', '')} с. [2-3 конкретні факти з цифрами]
Дії пілота: [коротка оцінка з урахуванням прошивки «1001»]
Висновок: [1 речення — підсумок]
===END_MESSENGER===

===REPORT===
Із загальних показників польоту стало відомо, що місія розпочинавалася {s.get('date', '')} о {s.get('start_time_local', '')} (UTC+2), загальною тривалістю {s.get('duration_s', '')} с. БпЛА досяг максимальної висоти {s.get('max_height_m', '')} м та виконав вихід на дистанцію {s.get('max_distance_m', '')} м.

1. ЗАГАЛЬНІ ДАНІ ПОЛЬОТУ

Дата: {s.get('date', '')}
Локальний час зльоту (UTC+2): {s.get('start_time_local', '')}
Локальний час {mission_word} (UTC+2): {s.get('end_time_local', '')}
Тривалість польоту: {s.get('duration_s', '')} с
Максимальна висота: {s.get('max_height_m', '')} м
Максимальна дистанція: {s.get('max_distance_m', '')} м
Прошивка «1001»: {"встановлена" if has_1001 else "не встановлена"}{f", антиспуфінг {antispoof_active}" if has_1001 else ""}
Статус завершення: {mission_word_cap} {'(Multiple Shocks / Spoofing)' if s.get('is_crash') and s.get('spoof_time_s') else '(аварія)' if s.get('is_crash') else '(нормальне завершення)'}

2. ШВИДКІСНІ ТА МЕТЕОДАНІ

[Аналіз: max {s.get('max_speed_ms', '')} м/с = {s.get('max_speed_kmh', '')} км/год. Якщо > 100 км/год — критична аномалія, поясни як наслідок спуфінгу. Опиши режими польоту.]

3. ХРОНОЛОГІЯ ТА МЕТРИКИ {'АВАРІЇ' if s.get('is_crash') else 'МІСІЇ'} (LOCAL TIME UTC+2)

[Хронологія. Використовуй реальні секунди і local-times з даних:
{'- ' + str(s.get('jamming_start_s')) + 'с (' + str(s.get('jamming_start_local')) + '): початок впливу РЕБ' if has_jamming else ''}
{'- ' + str(s.get('atti_times', [None])[0]) + 'с (' + str(s.get('atti_first_local')) + '): перехід ATTI' if has_atti else ''}
{'- ' + str(s.get('spoof_time_s')) + 'с (' + str(s.get('spoof_time_local')) + '): GNSS-спуфінг (0,0)' if has_spoof else ''}
- {s.get('duration_s', '')}с ({s.get('end_time_local', '')}): кінець запису]

4. {section4_title}

[Діагностика: {'РЕБ-джамінг + GNSS-спуфінг + механічні удари.' if has_jamming and has_spoof else 'поясни реальні причини з даних'}]

АНАЛІЗ ДІЙ ПІЛОТА

Кваліфікація дій: {pilot_qual}

{pilot_comment}

[Розгорни аналіз з урахуванням прошивки «1001» та фактичних даних логу.]

{technical_conclusion}
===END_REPORT==="""


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/normalize", methods=["POST"])
def normalize():
    """Convert comma-CSV (format 1) to semicolon-CSV (format 2) and return as download."""
    try:
        data = request.get_json(force=True)
        csv_text = data.get("csv", "")
        filename = data.get("filename", "flight.csv")

        if not csv_text:
            return jsonify({"error": "Порожній файл"}), 400

        normalized, was_converted = normalize_csv(csv_text)

        # Return as downloadable file
        out_name = filename.replace(".csv", "_normalized.csv")
        return Response(
            normalized,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/analyze", methods=["POST"])
def analyze():
    try:
        data = request.get_json(force=True)
        csv_text = data.get("csv", "")
        mission_type = data.get("mission_type", "ВТРАТА")
        filename = data.get("filename", "unknown.csv")

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return jsonify({"error": "ANTHROPIC_API_KEY не знайдено у .env файлі"}), 400
        if not csv_text:
            return jsonify({"error": "CSV-файл порожній"}), 400

        firmware_1001   = data.get("firmware_1001", "немає")
        antispoof_active = data.get("antispoof_active", None)

        _, rows = parse_csv(csv_text)
        if not rows:
            return jsonify({"error": "Не вдалося розпарсити CSV"}), 400

        metrics = extract_metrics(rows)
        summary = build_summary(metrics)
        prompt  = build_prompt(summary, filename, mission_type,
                               firmware_1001, antispoof_active)

        import anthropic as ant
        client = ant.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-opus-4-7",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text

        messenger, report = "", raw
        if "===MESSENGER===" in raw and "===END_MESSENGER===" in raw:
            messenger = raw.split("===MESSENGER===")[1].split("===END_MESSENGER===")[0].strip()
        if "===REPORT===" in raw and "===END_REPORT===" in raw:
            report = raw.split("===REPORT===")[1].split("===END_REPORT===")[0].strip()

        return jsonify({"messenger": messenger, "report": report, "summary": summary})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV", "development") == "development"
    app.run(host="0.0.0.0", port=port, debug=debug)
