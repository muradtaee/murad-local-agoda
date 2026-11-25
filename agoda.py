"""
Fake Agoda (Search API v4 + Book API v4 + precheck v2) for credit-hold QA.

Built from Trip-stick/agoda-shopping-worker and agoda-booking-worker (dev-k8s).
Credential URLs: base_url .../api/v4/  ratecheck_url .../api/v2/  booking_url .../api/
Control panel: /__admin/ui
"""
import asyncio
import json
import os
import random
import re
import threading
import time
from collections import deque
from datetime import date, datetime, timedelta, timezone

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ui import PAGE

S = {  # live settings (control panel)
    "tax_percent": float(os.getenv("TAX_PERCENT", "0")),
    "price_change_percent": float(os.getenv("PRICE_CHANGE_PERCENT", "10")),
    "default_scenario": "CONFIRM",
    "timeout_seconds": float(os.getenv("TIMEOUT_SLEEP_SECONDS", "40")),  # > credential booking_timeout (20)
    "slow_seconds": float(os.getenv("SLOW_SLEEP_SECONDS", "60")),        # < worker poll window (120)
    "nrf_cancel": "refuse",
    "only_listed": True,
}

# ---------------- persistence (hotels/settings/bookings survive a restart) ----------------
DATA_DIR = os.getenv("DATA_DIR", "data")
STATE_PATH = os.path.join(DATA_DIR, "state.json")
_SAVE_LOCK = threading.Lock()


def _load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            saved = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return
    PROPERTIES.update(saved.get("properties", {}))
    S.update(saved.get("settings", {}))
    for bid_str, b in saved.get("bookings", {}).items():
        BOOKINGS[int(bid_str)] = b


def _save_state():
    os.makedirs(DATA_DIR, exist_ok=True)
    payload = {
        "properties": PROPERTIES,
        "settings": S,
        "bookings": {str(bid): booking_view(b) for bid, b in BOOKINGS.items()},
    }
    tmp_path = f"{STATE_PATH}.tmp-{os.getpid()}"
    with _SAVE_LOCK:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp_path, STATE_PATH)

SCENARIOS = {
    "CONFIRM": "Will Confirm",
    "PENDING": "Will Pending",
    "ERROR": "Will Fail",
    "PRICE_CHANGED": "Will Change Price",
    "PRICE_MISMATCH": "Will Fail - Price Mismatch",
    "SOLD_OUT": "Will Sell Out",
    "TIMEOUT_NOT_BOOKED": "Will Timeout - Not Booked",
    "TIMEOUT_BOOKED_REFUNDABLE": "Will Timeout - Booked Refundable",
    "TIMEOUT_BOOKED_NRF": "Will Timeout - Booked Non-Refundable",
    "SLOW_CONFIRM": "Will Confirm Slowly",
}

# Agoda booking statuses <-> control panel words
TO_AGODA = {"booked": "BookingConfirmed", "pending": "BookingReceived", "canceled": "BookingCancelled"}
FROM_AGODA = {"BookingConfirmed": "booked", "BookingCharged": "booked", "BookingReceived": "pending",
              "BookingCancelled": "canceled", "BookingCancelledByCustomer": "canceled", "BookingRejected": "canceled"}

PROPERTIES = {}
BOOKINGS = {}
LOG = deque(maxlen=500)
SEARCHES = deque(maxlen=3000)

_load_state()


def prop(pid):
    return PROPERTIES.get(str(pid)) or {"scenario": S["default_scenario"], "price": 100}


def label(pid):
    s = prop(pid)["scenario"]
    return SCENARIOS[s] + (f" ({int(S['slow_seconds'])}s)" if s == "SLOW_CONFIRM" else "")


def is_nrf(pid):
    return prop(pid)["scenario"] == "TIMEOUT_BOOKED_NRF"


def r2(x):
    return round(float(x) + 1e-9, 2)


def per_night(pid, changed=False):
    """(inclusive, exclusive, tax) for one room, one night."""
    p = float(prop(pid)["price"])
    if changed and prop(pid)["scenario"] == "PRICE_CHANGED":
        p = r2(p * (1 + S["price_change_percent"] / 100))
    tax = r2(p * S["tax_percent"] / 100)
    return r2(p + tax), p, tax


def iso_day(d):
    return d.strftime("%Y-%m-%dT00:00:00")


def room_plan(pid, check_in, check_out, rooms, currency, changed=False):
    ci, co = date.fromisoformat(check_in), date.fromisoformat(check_out)
    nights = max((co - ci).days, 1)
    inc, exc, tax = per_night(pid, changed)
    total_inc, total_exc, total_tax = r2(inc * nights * rooms), r2(exc * nights * rooms), r2(tax * nights * rooms)
    if is_nrf(pid):
        policy = {"code": "365D100P_100P", "translatedCancellationText": "This booking is non-refundable.",
                  "date": [{"onward": iso_day(date.today()), "rate": {"inclusive": inc * nights}}]}
        free = False
    else:
        deadline = ci - timedelta(days=3)
        policy = {"code": "3D100P_100P",
                  "translatedCancellationText": f"Free cancellation before {deadline.isoformat()}.",
                  "date": [{"before": iso_day(deadline), "rate": {"inclusive": 0}},
                           {"onward": iso_day(deadline), "rate": {"inclusive": inc * nights}}]}
        free = True
    room_id = int(str(pid)[-6:] or 1)
    return {
        "roomId": room_id,
        "blockId": f"MOCK-{pid}-{room_id}",
        "translatedRoomName": f"{label(pid)} - Double Room",
        "normalBedding": 2,
        "extraBeds": 0,
        "remainingRooms": 10,
        "freeCancellation": free,
        "rate": {"currency": currency, "inclusive": inc, "exclusive": exc, "tax": tax, "fees": 0},
        "totalPayment": {"currency": currency, "inclusive": total_inc, "exclusive": total_exc,
                         "tax": total_tax, "fees": 0},
        "dailyRate": [{"date": iso_day(ci + timedelta(days=i)), "inclusive": inc, "exclusive": exc,
                       "tax": tax, "fees": 0} for i in range(nights)],
        "cancellationPolicy": policy,
        "benefits": [],
        "surcharges": [],
    }


def err(message, id_=999, sub_id="0", status=200):
    return JSONResponse({"errorMessage": {"id": id_, "subId": sub_id, "message": message}}, status_code=status)


def booking_view(b):
    return {k: v for k, v in b.items() if not k.startswith("_")}


app_inner = FastAPI(title="Fake Agoda")


@app_inner.middleware("http")
async def log_requests(request: Request, call_next):
    raw = await request.body()
    entry = {"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "method": request.method,
             "path": request.url.path, "query": str(request.url.query)[:2000],
             "body": raw.decode(errors="ignore")[:4000]}
    LOG.append(entry)
    resp = await call_next(request)
    entry["status"] = resp.status_code
    return resp


# ---------------- Search API ----------------

@app_inner.post("/api/v4/property/availability")
async def availability(request: Request):
    c = (await request.json()).get("criteria", {})
    ids = [str(i) for i in c.get("propertyIds", [])]
    rooms = int(c.get("rooms") or 1)
    currency = c.get("currency", "USD")
    listed = [i for i in ids if i in PROPERTIES]
    answer = listed if S["only_listed"] else ids
    out = [{"propertyId": int(i), "propertyName": label(i), "rooms": [
        room_plan(i, c["checkIn"], c["checkOut"], rooms, currency)]} for i in answer]
    SEARCHES.append({"time": datetime.now(timezone.utc).strftime("%H:%M:%S"), "checkin": c.get("checkIn"),
                     "currency": currency, "asked": len(ids), "yours": listed, "returned": len(out)})
    return {"searchId": random.randint(10**8, 10**9), "properties": out}


# ---------------- Precheck (rate check) ----------------

@app_inner.post("/api/v2/prebooking/precheck")
async def precheck(request: Request):
    d = (await request.json()).get("precheckDetails", {})
    pid = str(d.get("property", {}).get("propertyId"))
    room = (d.get("property", {}).get("rooms") or [{}])[0]
    s = prop(pid)["scenario"]
    if s == "SOLD_OUT":
        return {"status": 502, "message": "Room is sold out"}
    requested = float((room.get("rate") or {}).get("inclusive") or 0)
    expected = per_night(pid, changed=True)[0]
    if abs(requested - expected) > 0.01:
        fresh = room_plan(pid, d["checkIn"], d["checkOut"], int(room.get("count") or 1),
                          room.get("currency", "USD"), changed=True)
        return {"status": 501, "message": "Room has allotment still available but the rate has changed",
                "errorList": [{"priceMismatchData": {
                    "requestedRate": requested, "newRate": expected,
                    "searchResponse": {"searchId": random.randint(10**8, 10**9),
                                       "properties": [{"propertyId": int(pid), "rooms": [fresh]}]}}}]}
    return {"status": 200, "message": "Available", "precheckDetails": {"propertyId": int(pid)}}


# ---------------- Book API ----------------

def new_booking(details, customer, status):
    bid = int(time.time() * 1000) % 10**9 + random.randint(0, 999)
    pid = str(details["property"]["propertyId"])
    room = details["property"]["rooms"][0]
    b = {"id": bid, "itineraryId": bid + 7, "tag": details.get("tag", ""), "status": status,
         "propertyId": int(pid), "checkIn": details.get("checkIn"), "checkOut": details.get("checkOut"),
         "rate": {"currency": room.get("currency"), "inclusive": room.get("rate", {}).get("inclusive")},
         "rooms": room.get("count", 1), "guestName": f"{customer.get('firstName', '')} {customer.get('lastName', '')}",
         "supplierReference": f"MOCK{bid}", "hotelConfirmationNumber": f"HCN{bid}",
         "_confirm_at": None}
    BOOKINGS[bid] = b
    _save_state()
    return b


@app_inner.post("/api/v4/book")
async def book(request: Request):
    body = await request.json()
    details = body.get("bookingDetails", {})
    pid = str(details.get("property", {}).get("propertyId"))
    s = prop(pid)["scenario"]
    if s == "ERROR":
        return err("Booking failed at the property (mock)", 909, "1")
    if s == "PRICE_MISMATCH":
        return err("Price has changed. Please search again.", 907, "2")
    if s == "TIMEOUT_NOT_BOOKED":
        await asyncio.sleep(S["timeout_seconds"])
        return err("Mock timeout, nothing booked", 500, "0", status=504)
    if s == "PENDING":
        b = new_booking(details, body.get("customerDetail", {}), "BookingReceived")
    elif s == "SLOW_CONFIRM":
        b = new_booking(details, body.get("customerDetail", {}), "BookingReceived")
        b["_confirm_at"] = time.time() + S["slow_seconds"]
    else:
        b = new_booking(details, body.get("customerDetail", {}), "BookingConfirmed")
    if s in ("TIMEOUT_BOOKED_REFUNDABLE", "TIMEOUT_BOOKED_NRF"):
        await asyncio.sleep(S["timeout_seconds"])  # booked here, but the worker gives up first
    return {"bookingDetails": [{"id": b["id"], "itineraryId": b["itineraryId"], "processing": True}]}


def refresh(b):
    if b.get("_confirm_at") and time.time() >= b["_confirm_at"] and b["status"] == "BookingReceived":
        b["status"], b["_confirm_at"] = "BookingConfirmed", None
        _save_state()
    return b


def find(ids=None, tags=None):
    ids = {int(i) for i in (ids or []) if str(i).isdigit()}
    tags = set(tags or [])
    return [booking_view(refresh(b)) for b in BOOKINGS.values() if b["id"] in ids or (tags and b["tag"] in tags)]


@app_inner.post("/api/v4/bookingreport/bookingdetail")
async def booking_detail(request: Request):
    b = await request.json()
    return {"bookings": find(b.get("bookingIds"), b.get("tags"))}


@app_inner.post("/api/v4/bookingreport/booklist")
async def booking_list(request: Request):
    b = await request.json()
    return {"bookings": find(b.get("bookingIds"), b.get("tags"))}


@app_inner.post("/api/v4/postBooking/cancel")
async def cancel(request: Request):
    bid = int((await request.json()).get("bookingId", 0))
    b = BOOKINGS.get(bid)
    if not b:
        return err("Booking not found", 404, "1")
    if b["status"] in ("BookingCancelled", "BookingCancelledByCustomer"):
        return err("Booking already cancelled", 104, "104")
    nrf = is_nrf(b["propertyId"])
    if nrf and S["nrf_cancel"] == "refuse":
        return err("Booking is non-refundable and cannot be cancelled", 109, "109")
    nights = max((date.fromisoformat(b["checkOut"]) - date.fromisoformat(b["checkIn"])).days, 1)
    refund = 0 if nrf else r2(float(b["rate"]["inclusive"] or 0) * nights * int(b["rooms"] or 1))
    b["_cancel_ref"] = random.randint(10**6, 10**7)
    _save_state()
    return {"cancellationSummary": {"reference": b["_cancel_ref"],
                                    "refundRate": [{"currency": b["rate"]["currency"], "inclusive": refund}]}}


@app_inner.post("/api/v4/postBooking/confirmcancel")
async def confirm_cancel(request: Request):
    bid = int((await request.json()).get("bookingId", 0))
    b = BOOKINGS.get(bid)
    if not b:
        return err("Booking not found", 404, "1")
    b["status"] = "BookingCancelledByCustomer"
    _save_state()
    return {}


# ---------------- admin + control panel ----------------

@app_inner.get("/__admin")
async def admin():
    return {
        "scenarios": SCENARIOS,
        "properties": {pid: {**p, "label": label(pid)} for pid, p in PROPERTIES.items()},
        "itineraries": [{"itinerary_id": str(b["id"]), "property_id": str(b["propertyId"]),
                         "affiliate_reference_id": b["tag"],
                         "rooms": [{"status": FROM_AGODA.get(refresh(b)["status"], b["status"])}]}
                        for b in BOOKINGS.values()],
    }


@app_inner.get("/__admin/settings")
async def get_settings():
    return S


@app_inner.post("/__admin/settings")
async def set_settings(request: Request):
    b = await request.json()
    for k in ("tax_percent", "price_change_percent", "timeout_seconds", "slow_seconds"):
        if k in b:
            S[k] = float(b[k])
    if b.get("default_scenario") in SCENARIOS:
        S["default_scenario"] = b["default_scenario"]
    if b.get("nrf_cancel") in ("refuse", "allow"):
        S["nrf_cancel"] = b["nrf_cancel"]
    if "only_listed" in b:
        S["only_listed"] = bool(b["only_listed"])
    _save_state()
    return S


@app_inner.post("/__admin/properties/{pid}")
async def set_property(pid: str, request: Request):
    b = await request.json()
    if b.get("scenario") not in SCENARIOS:
        return JSONResponse({"error": f"scenario must be one of {list(SCENARIOS)}"}, status_code=400)
    PROPERTIES[pid] = {"scenario": b["scenario"], "price": b.get("price", prop(pid)["price"])}
    _save_state()
    return {pid: {**PROPERTIES[pid], "label": label(pid)}}


@app_inner.delete("/__admin/properties/{pid}")
async def remove_property(pid: str):
    PROPERTIES.pop(pid, None)
    _save_state()
    return {"ok": True}


@app_inner.post("/__admin/itineraries/{key}/status")
async def set_status(key: str, request: Request):
    st = (await request.json()).get("status")
    if st not in TO_AGODA:
        return JSONResponse({"error": "status must be booked, pending or canceled"}, status_code=400)
    hits = [b for b in BOOKINGS.values() if key in (str(b["id"]), b["tag"])]
    for b in hits:
        b["status"], b["_confirm_at"] = TO_AGODA[st], None
    _save_state()
    return [booking_view(b) for b in hits]


@app_inner.get("/__admin/searches")
async def searches():
    summary = {}
    for x in SEARCHES:
        g = summary.setdefault((x["checkin"], x["currency"]), {
            "checkin": x["checkin"], "currency": x["currency"], "first": x["time"], "last": x["time"],
            "batches": 0, "asked": 0, "yours": [], "returned": 0})
        g["last"] = x["time"]; g["batches"] += 1; g["asked"] += x["asked"]; g["returned"] += x["returned"]
        g["yours"] += [i for i in x["yours"] if i not in g["yours"]]
    return list(summary.values())[::-1]


@app_inner.get("/__admin/log")
async def admin_log(path: str = "", limit: int = 100):
    return [e for e in LOG if path in e["path"]][-limit:]


@app_inner.post("/__admin/reset")
async def reset():
    PROPERTIES.clear(); BOOKINGS.clear(); LOG.clear(); SEARCHES.clear()
    _save_state()
    return {"ok": True}


@app_inner.get("/__admin/ui", response_class=HTMLResponse)
async def ui():
    return PAGE.replace("Fake supplier control", "Fake Agoda supplier control")


@app_inner.get("/")
async def root():
    return {"ok": True, "panel": "/__admin/ui"}


class CollapseSlashes:
    """Credential URLs end with '/', so paths can arrive as /api/v4//property/..."""
    def __init__(self, inner):
        self.inner = inner

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            scope = dict(scope, path=re.sub(r"/{2,}", "/", scope["path"]))
        await self.inner(scope, receive, send)


app = CollapseSlashes(app_inner)
