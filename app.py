import os
import io
import csv
import json
import re
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
    t, h, spd_ms, spd_kmh, dist, sats, gpslvl, bat = [], [], [], [], [], [], [], []
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
        gpslvl.append(int(gv(r, "gpslevel")))

    return dict(t=t, h=h, spd_ms=spd_ms, spd_kmh=spd_kmh, dist=dist,
                sats=sats, gpslvl=gpslvl, bat=bat, pitch=pitch, roll=roll, hdg=hdg,
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

    # Compass anomalies: heading jumps >60° per step
    hdg = m["hdg"]
    compass_jump_idx = []
    for i in range(1, len(hdg)):
        diff = abs(hdg[i] - hdg[i-1])
        if diff > 180:
            diff = 360 - diff
        if diff > 60:
            compass_jump_idx.append(i)
    spinning_detected = False
    for i in range(len(compass_jump_idx) - 2):
        t_start = t[compass_jump_idx[i]]
        t_end   = t[compass_jump_idx[i + 2]]
        if t_end - t_start <= 3:
            spinning_detected = True
            break

    # Data Recorder File Index (internal DJI flight counter)
    flight_index = None
    for msg in m["msgs"]:
        match = re.search(r"Data Recorder File Index is (\d+)", msg)
        if match:
            flight_index = int(match.group(1))
            break

    # Flight modes used during flight
    MODE_MAP = {
        "sport":  "Sport",
        "cine":   "Cine",
        "p-gps":  "Normal",
        "opti":   "Normal",
        "atti":   "ATTI",
    }
    used_modes = []
    for st in m["states"]:
        st_lc = st.lower()
        for key, label in MODE_MAP.items():
            if key in st_lc and label not in used_modes:
                used_modes.append(label)

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
        "flight_index": flight_index,
        "flight_modes": used_modes,
        "compass_jumps": len(compass_jump_idx),
        "compass_jump_t": [round(t[i], 1) for i in compass_jump_idx],
        "spinning_detected": spinning_detected,
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


def _deg_to_compass(deg):
    """Convert 0–360° azimuth to 8-point compass label (Ukrainian)."""
    dirs = ["Пн", "Пн-Сх", "Сх", "Пд-Сх", "Пд", "Пд-Зх", "Зх", "Пн-Зх"]
    return dirs[round(deg / 45) % 8]


def build_prediction_prompt(dr, fs, home_lat, home_lon,
                            wind_dir_deg, wind_speed_ms, filename):
    """
    Build AI prompt using pre-computed dead reckoning (dr) and flight summary (fs).

    Physics model already applied by JS (2-phase):
      Phase 1 — Active flight:  GPS ground-speed integrated from GPS-loss → peak height.
                Ground speed is GPS-derived (already includes wind effect).
      Phase 2 — Free fall:      t_fall = H / v_descent; drift = V_wind × t_fall.

    dr keys:
      lat, lon                  — final predicted landing point (after wind drift)
      last_valid_lat/lon        — last valid GPS fix position
      last_valid_t              — timestamp of last GPS fix
      no_gps                    — True if entire flight had no GPS
      dist_own_m                — horizontal distance integrated in Phase 1
      dr_duration_s             — total duration from GPS loss to end of telemetry
      incident_lat, incident_lon— position at peak height (Phase 1 end)
      incident_h_m              — height AGL at the incident point (metres)
      fall_time_s               — physics-based free-fall duration (H / descent_rate)
      descent_rate_ms           — descent rate used (measured or 2.2 m/s default)
      wind_drift_m              — horizontal wind drift during free fall (metres)
      wind_to_deg               — downwind azimuth (degrees)
    """
    # ── DR fields ──────────────────────────────────────────────────────────
    dr_lat         = dr.get("lat",             home_lat)
    dr_lon         = dr.get("lon",             home_lon)
    gps_lat        = dr.get("last_valid_lat",  home_lat)
    gps_lon        = dr.get("last_valid_lon",  home_lon)
    gps_t          = dr.get("last_valid_t",    None)
    no_gps         = dr.get("no_gps",          False)
    manual_home    = dr.get("manual_home",     False)
    dr_dist_own    = dr.get("dist_own_m",      0)
    dr_dur_s       = dr.get("dr_duration_s",   0)
    incident_lat   = dr.get("incident_lat",    dr.get("peak_lat",  dr_lat))
    incident_lon   = dr.get("incident_lon",    dr.get("peak_lon",  dr_lon))
    incident_h     = dr.get("incident_h_m",   dr.get("peak_h_m",  0))
    fall_time_s    = dr.get("fall_time_s",     dr.get("fall_duration_s", 0))
    descent_rate   = dr.get("descent_rate_ms", 2.2)
    wind_drift_m   = dr.get("wind_drift_m",    0)
    wind_to        = dr.get("wind_to_deg",     int((wind_dir_deg + 180) % 360))

    # ── Flight summary fields ──────────────────────────────────────────────
    last_h      = fs.get("last_height_m",    0)
    last_z      = fs.get("last_vspeed_ms",   0)
    last_spd    = fs.get("last_hspeed_ms",   0)
    last_hdg    = fs.get("last_heading_deg", 0)
    last_pitch  = fs.get("last_pitch_deg",   0)
    last_roll   = fs.get("last_roll_deg",    0)
    flight_s    = fs.get("flight_duration_s", 0)
    max_h       = fs.get("max_height_m",     last_h)

    post_avg_hdg = fs.get("post_gps_avg_hdg_deg",   last_hdg)
    post_avg_spd = fs.get("post_gps_avg_spd_ms",    last_spd)
    post_avg_z   = fs.get("post_gps_avg_vspeed_ms", last_z)
    post_dur     = fs.get("post_gps_duration_s",    dr_dur_s)

    # ── GPS context block ──────────────────────────────────────────────────
    # Three distinct scenarios:
    #   manual_home=True  → log may have GPS, but operator anchored with real coords
    #                        (spoofed GPS, known real home, etc.)
    #   no_gps=True (only) → entire flight had zero GPS fix
    #   else               → GPS present in log; DR starts from last valid fix
    if manual_home:
        gps_block = (
            f"Стартова точка задана оператором ВРУЧНУ: {home_lat:.7f}, {home_lon:.7f}\n"
            f"  GPS-координати з логу ІГНОРУЮТЬСЯ (можливий спуфінг або помилковий сигнал).\n"
            f"  Dead reckoning виконано від ручної точки старту через весь маршрут ({flight_s}с).\n"
            f"  ★ Телеметрія курсу та швидкості з логу залишається валідною."
        )
        phase1_block = (
            f"ФАЗА 1 — Активний політ від ручної стартової точки ({dr_dur_s}с, {dr_dist_own}м):\n"
            f"  Інтегровано всю телеметрію від t=0 → пікова висота.\n"
            f"  Середній курс: {post_avg_hdg:.1f}°, середня швидкість: {post_avg_spd:.2f} м/с\n"
            f"  ★ Наземна швидкість — GPS-похідна (якщо GPS не спуфінгована на швидкість),\n"
            f"    вітер вже закладено у вектор руху. Якщо вітер > швидкість дрона → рух назад.\n"
            f"  Розрахована позиція інциденту: {incident_lat:.7f}, {incident_lon:.7f} "
            f"(висота {incident_h}м)\n"
            f"  ⚠ Похибка DR зростає зі збільшенням тривалості інтеграції від ручної точки."
        )
    elif no_gps:
        gps_block = (
            f"⚠ ПОЛІТ БЕЗ GPS — весь маршрут без позиціонування.\n"
            f"  Стартова точка вказана оператором вручну: {home_lat:.7f}, {home_lon:.7f}\n"
            f"  Dead reckoning охоплює весь політ ({flight_s}с). "
            f"Накопичена похибка може бути суттєвою."
        )
        phase1_block = (
            f"ФАЗА 1 — Активний політ (лог без GPS, {dr_dur_s}с):\n"
            f"  Інтегровано весь маршрут від стартової точки → пікова висота.\n"
            f"  Середній курс: {post_avg_hdg:.1f}°, середня швидкість: {post_avg_spd:.2f} м/с\n"
            f"  Розрахована позиція інциденту: {incident_lat:.7f}, {incident_lon:.7f} "
            f"(висота ~{incident_h}м)"
        )
    else:
        gps_block = (
            f"Остання валідна GPS: {gps_lat:.7f}, {gps_lon:.7f}  (t={gps_t}с)\n"
            f"  Після цього GPS-сигнал відсутній — дрон летів без позиціонування."
        )
        phase1_block = (
            f"ФАЗА 1 — Активний політ після GPS-втрати ({post_dur}с):\n"
            f"  Інтегровано телеметрію від GPS-фіксу → пікова висота ({dr_dist_own}м).\n"
            f"  Середній курс: {post_avg_hdg:.1f}°, середня швидкість: {post_avg_spd:.2f} м/с\n"
            f"  ★ Наземна швидкість — GPS-похідна, вітер вже закладено у вектор руху.\n"
            f"    Якщо вітер > швидкість дрона, апарат фактично відкидало назад.\n"
            f"  Позиція інциденту (пік): {incident_lat:.7f}, {incident_lon:.7f} "
            f"(висота {incident_h}м)"
        )

    phase2_block = (
        f"ФАЗА 2 — Некерований спуск (фізична модель):\n"
        f"  Час падіння: {incident_h}м ÷ {descent_rate:.1f}м/с = {fall_time_s:.1f}с\n"
        f"  Зміщення вітром: {wind_speed_ms}м/с × {fall_time_s:.1f}с = {wind_drift_m}м "
        f"в напрямку {wind_to}° ({_deg_to_compass(wind_to)})\n"
        f"  ★ Розрахована точка приземлення: {dr_lat:.7f}, {dr_lon:.7f}"
    )

    # ── Descent rate hint based on last measured V_z ──────────────────
    # If V_z at end of telemetry is a meaningful descent rate, use it;
    # otherwise fall back to the model-computed default.
    measured_vz = abs(last_z) if last_z < -0.3 else None
    vz_hint = (
        f"Телеметрія зафіксувала вертикальну швидкість {last_z:.2f} м/с в останні секунди "
        f"→ це вказує на {'керований повільний спуск (авто-посадка/RTH)' if abs(last_z) < 2.5 else 'швидкий некерований спуск (пошкодження двигунів/РЕБ-падіння)'}. "
        f"Алгоритм використав {descent_rate:.1f} м/с — {'збігається з виміряним.' if measured_vz and abs(measured_vz - descent_rate) < 0.8 else 'перевір чи потрібна корекція.'}"
        if last_z != 0 else
        f"Вертикальна швидкість в останні секунди = 0 (телеметрія обрізана до початку спуску). "
        f"Алгоритм використав {descent_rate:.1f} м/с за замовчуванням."
    )

    return f"""Ти — досвідчений фахівець пошуково-рятувальних операцій з БпЛА.
Твоє завдання — ПЕРЕВІРИТИ фізичну модель, яку вже виконав алгоритм, і за потреби скоригувати результат.

════════════════════════════════════════════════════════
ПЛАТФОРМА — ХАРАКТЕРИСТИКИ ДРОНІВ
════════════════════════════════════════════════════════
Лог-файл сумісний з такими платформами (DJI Airdata формат):

  DJI Mavic 3 / 3T / 3E (~895 г)
    Макс. швидкість: ~21 м/с | Крейсерська: 10–14 м/с
    Failsafe-спуск (авто-посадка RTH): 1.5–2.0 м/с
    Некерований спуск (пошкодження/РЕБ): 4–7 м/с
    ATTI-режим: дрейфує зі швидкістю ≈ вітру, кут нахилу збільшується
    Аеродинамічний знос у вільному падінні: суттєвий (площа ~0.12 м²)

  DJI Matrice 30 / 30T (~3.77 кг)
    Макс. швидкість: ~23 м/с
    Failsafe-спуск: 2.0–3.0 м/с
    Некерований спуск: 5–9 м/с (важча платформа)
    Більша маса = більший горизонтальний знос вітром при некерованому падінні

  DJI Matrice 300 RTK (~6.3 кг без навантаження)
    Макс. швидкість: ~23 м/с
    Failsafe-спуск: 1.5–3.0 м/с
    Велика лобова площа → сильний боковий знос при падінні
    З підвісом маса може сягати 9+ кг → прискорений спуск

  DJI Matrice 4E / 4T (~1.5 кг)
    Характеристики близькі до Mavic 3 Enterprise
    Failsafe-спуск: 1.5–2.5 м/с | Некерований: 4–7 м/с

  Autel EVO Max 4N / 4T (~1.35 кг)
    Макс. швидкість: ~20 м/с | Схожий на Mavic-клас
    Failsafe-спуск: 1.5–2.2 м/с | Некерований: 4–6 м/с

СЦЕНАРІЇ ВТРАТИ КЕРУВАННЯ (впливають на швидкість спуску):
  • РЕБ-придушення → перехід в ATTI → дрейф зі швидкістю вітру → автопосадка 1.5–2.5 м/с
  • Повна втрата зв'язку → RTH або автопосадка → 1.5–3.0 м/с
  • Пошкодження двигунів / влучання → некерований спуск → 4–9 м/с
  • Критичний розряд АКБ → примусова посадка → 1.5–2.0 м/с
  ОЗНАКИ сценарію в телеметрії: pitch/roll > 30° = некерований; V_z < -2.5 = швидке падіння

════════════════════════════════════════════════════════
ДАНІ ПОЛЬОТУ  [{filename}]
════════════════════════════════════════════════════════
Стартова точка: {home_lat:.7f}, {home_lon:.7f}
{gps_block}
Тривалість: {flight_s}с | Макс. висота: {max_h}м

ВІТЕР: {wind_speed_ms} м/с З напрямку {wind_dir_deg}° → дме В напрямку {wind_to}° ({_deg_to_compass(wind_to)})

════════════════════════════════════════════════════════
РОЗРАХУНОК АЛГОРИТМУ (2-фазна модель)
════════════════════════════════════════════════════════
{phase1_block}

{phase2_block}

ОСТАННІ СЕКУНДИ ТЕЛЕМЕТРІЇ:
  Висота: {last_h}м | V_верт: {last_z:.2f}м/с | V_гор: {last_spd:.2f}м/с
  Курс: {last_hdg:.1f}° | Pitch: {last_pitch:.1f}° | Roll: {last_roll:.1f}°

АНАЛІЗ ШВИДКОСТІ СПУСКУ:
  {vz_hint}
  Pitch={last_pitch:.1f}° / Roll={last_roll:.1f}° → {"⚠ НЕСТАБІЛЬНИЙ політ (>30°) — некерований спуск, використовуй 4–8 м/с" if abs(last_pitch) > 30 or abs(last_roll) > 30 else "стабільний — підтверджує керований/ATTI спуск"}

════════════════════════════════════════════════════════
ТВОЄ ЗАВДАННЯ
════════════════════════════════════════════════════════
1. ВИЗНАЧ СЦЕНАРІЙ за pitch/roll, V_z і контекстом:
   керований RTH/автопосадка → 1.5–2.5 м/с
   ATTI-дрейф з РЕБ → 1.5–2.5 м/с + суттєвий боковий дрейф вже в активній фазі
   пошкодження/некерований → 4–9 м/с (коротший час падіння, менший знос)

2. ПЕРЕВІР Фазу 1 (активний політ):
   — Позиція інциденту: {incident_lat:.5f}, {incident_lon:.5f} — чи правдоподібна?
   — ПРИНЦИП «бігова доріжка»: якщо вітер ({wind_speed_ms} м/с) > швидкість дрона
     ({post_avg_spd:.2f} м/с) і вітер ПРОТИ курсу → GPS-швидкість відображає рух НАЗАД.
     Вітер з {wind_dir_deg}°, курс дрона {post_avg_hdg:.1f}° — оціни результуючий вектор.

3. ПЕРЕВІР Фазу 2 (спуск):
   — Обраний алгоритмом темп спуску: {descent_rate:.1f} м/с — чи відповідає сценарію?
   — Час падіння {fall_time_s:.1f}с з {incident_h}м — реалістично для цього типу дрона?
   — Зміщення {wind_drift_m}м у напрямку {wind_to}° — перевір напрямок та величину.
   — Для важких платформ (Matrice 30/300) враховуй більший горизонтальний знос.

4. СКОРИГУЙ і дай ОСТАТОЧНІ координати.
   {
        "⚠ Ручна стартова точка — GPS з логу проігноровано. DR інтегровано від t=0, похибка зростає з тривалістю польоту. Радіус невизначеності 200–500м залежно від тривалості та умов."
        if manual_home else
        ("⚠ Лог без GPS — DR охоплює весь маршрут. Радіус невизначеності 300–600м."
        if no_gps else "")
    }

Відповідай у такому форматі:

===COORDS===
[остаточна_широта],[остаточна_довгота]
===END_COORDS===

===EXPLANATION===
**Сценарій та тип спуску:**
[Визначений сценарій на основі телеметрії. Тип дрона, очікувана швидкість спуску.]

**Перевірка Фази 1 (активний політ):**
[Оцінка позиції інциденту. Аналіз принципу «бігова доріжка» для даного вітру та швидкості.]

**Перевірка Фази 2 (спуск):**
[Скоригований темп спуску, час, знос вітром. Чи змінено відносно алгоритму і чому.]

**Остаточна точка приземлення:**
Алгоритм: {dr_lat:.7f}, {dr_lon:.7f}
Скоригована: [широта], [довгота]
Відхилення від алгоритму: [X]м в напрямку [Y]°

**Зона пошуку:**
Центр: [координати] | Радіус: [X]м
[Обґрунтування — звідки береться радіус невизначеності]

**Пріоритет пошуку:**
[Азимут від старту, відстань, конкретні орієнтири]
===END_EXPLANATION==="""


@app.route("/predict", methods=["POST"])
def predict():
    """AI-assisted UAV landing point prediction."""
    try:
        data = request.get_json(force=True)
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return jsonify({"error": "ANTHROPIC_API_KEY не знайдено у .env"}), 400

        dr            = data.get("dead_reckoning", {})
        fs            = data.get("flight_summary",  {})
        home_lat      = float(data.get("home_lat",     0))
        home_lon      = float(data.get("home_lon",     0))
        wind_dir_deg  = float(data.get("wind_dir_deg", 0))
        wind_speed_ms = float(data.get("wind_speed_ms",0))
        filename      = data.get("filename", "unknown.csv")

        prompt = build_prediction_prompt(
            dr, fs, home_lat, home_lon,
            wind_dir_deg, wind_speed_ms, filename
        )

        import anthropic as ant
        client = ant.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-opus-4-7",
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text

        pred_lat, pred_lon = None, None
        m = re.search(r'===COORDS===\s*(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)\s*===END_COORDS===',
                      raw, re.DOTALL)
        if m:
            pred_lat = float(m.group(1))
            pred_lon = float(m.group(2))

        explanation = raw
        if "===EXPLANATION===" in raw and "===END_EXPLANATION===" in raw:
            explanation = (raw.split("===EXPLANATION===")[1]
                              .split("===END_EXPLANATION===")[0].strip())

        return jsonify({"pred_lat": pred_lat, "pred_lon": pred_lon,
                        "explanation": explanation})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
