import os
import sqlite3
import secrets
import hashlib
from datetime import date, datetime, timedelta
from calendar import monthrange

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# Path to SQLite database within the project
DATABASE_PATH = os.path.join(os.path.dirname(__file__), "storage.db")

# In-memory session store; maps session tokens to {user: row, expiry: datetime}
sessions = {}


def init_db():
    """
    Initialise the SQLite database and create tables if they do not exist.
    Tables: units, tenants, occupancies, users, login_records, invoices, invoice_items.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    # Units table
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS units (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            daily_rate REAL NOT NULL
        )
        """
    )
    # Tenants table
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT,
            phone TEXT
        )
        """
    )
    # Occupancies table
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS occupancies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            unit_id INTEGER NOT NULL,
            tenant_id INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT,
            daily_rate REAL NOT NULL,
            FOREIGN KEY(unit_id) REFERENCES units(id),
            FOREIGN KEY(tenant_id) REFERENCES tenants(id)
        )
        """
    )
    # Users table
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL
        )
        """
    )
    # Login records
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS login_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            login_time TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    # Invoices table
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            total_amount REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            paid_at TEXT,
            FOREIGN KEY(tenant_id) REFERENCES tenants(id),
            UNIQUE(tenant_id, year, month)
        )
        """
    )
    # Invoice items table
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL,
            unit_name TEXT NOT NULL,
            days INTEGER NOT NULL,
            daily_rate REAL NOT NULL,
            amount REAL NOT NULL,
            FOREIGN KEY(invoice_id) REFERENCES invoices(id)
        )
        """
    )
    conn.commit()
    conn.close()


def get_db_connection():
    """Return a new connection to the SQLite database."""
    return sqlite3.connect(DATABASE_PATH)


def hash_password(password: str) -> str:
    """Hash a password using SHA-256."""
    return hashlib.sha256(password.encode()).hexdigest()


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against a stored hash."""
    return hash_password(password) == password_hash


def add_user(username: str, password: str, role: str = "staff"):
    """Add a new user to the database."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
        (username, hash_password(password), role),
    )
    conn.commit()
    conn.close()


def get_user_by_username(username: str):
    """Retrieve a user row by username."""
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE username = ?", (username,))
    user = c.fetchone()
    conn.close()
    return user


def add_login_record(user_id: int):
    """Record a login for a user."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO login_records (user_id, login_time) VALUES (?, ?)",
        (user_id, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def create_default_admin():
    """Create a default admin user if no users exist."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users")
    count = c.fetchone()[0]
    conn.close()
    if count == 0:
        add_user("admin", "admin123", role="admin")


def add_unit(name: str, daily_rate: float):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("INSERT INTO units (name, daily_rate) VALUES (?, ?)", (name, daily_rate))
    conn.commit()
    conn.close()


def update_unit_rate(unit_id: int, daily_rate: float):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE units SET daily_rate = ? WHERE id = ?", (daily_rate, unit_id))
    conn.commit()
    conn.close()


def add_tenant(name: str, email: str, phone: str):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO tenants (name, email, phone) VALUES (?, ?, ?)",
        (name, email, phone),
    )
    conn.commit()
    conn.close()


def assign_unit(unit_id: int, tenant_id: int, start: date):
    """Assign a unit to a tenant starting on the given date."""
    conn = get_db_connection()
    c = conn.cursor()
    # fetch current rate
    c.execute("SELECT daily_rate FROM units WHERE id = ?", (unit_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        raise ValueError("Unit not found")
    rate = row[0]
    c.execute(
        "INSERT INTO occupancies (unit_id, tenant_id, start_date, end_date, daily_rate) VALUES (?, ?, ?, NULL, ?)",
        (unit_id, tenant_id, start.isoformat(), rate),
    )
    conn.commit()
    conn.close()


def end_occupancy(occupancy_id: int, end: date):
    """Set an occupancy's end date to mark the unit as vacant."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE occupancies SET end_date = ? WHERE id = ?", (end.isoformat(), occupancy_id))
    conn.commit()
    conn.close()


def fetch_units():
    """
    Fetch all units along with occupancy information. For occupied units, also
    return the tenant name, start and end dates, and compute days remaining
    until lease end (if end_date is set).
    """
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(
        """
        SELECT u.id, u.name, u.daily_rate,
               o.id AS occupancy_id, o.tenant_id,
               t.name AS tenant_name,
               o.start_date,
               o.end_date
        FROM units u
        LEFT JOIN occupancies o ON u.id = o.unit_id AND o.end_date IS NULL
        LEFT JOIN tenants t ON o.tenant_id = t.id
        ORDER BY u.id
        """
    )
    rows = c.fetchall()
    conn.close()
    today_date = date.today()
    units = []
    for row in rows:
        u = dict(row)
        # compute days_remaining
        end_str = u.get("end_date")
        if end_str:
            try:
                end_dt = datetime.strptime(end_str, "%Y-%m-%d").date()
                u["days_remaining"] = (end_dt - today_date).days
            except Exception:
                u["days_remaining"] = None
        else:
            u["days_remaining"] = None
        units.append(u)
    return units


def fetch_tenants():
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM tenants ORDER BY name")
    tenants = c.fetchall()
    conn.close()
    return tenants


def fetch_active_occupancies():
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(
        """
        SELECT o.*, u.name AS unit_name, t.name AS tenant_name
        FROM occupancies o
        JOIN units u ON o.unit_id = u.id
        JOIN tenants t ON o.tenant_id = t.id
        WHERE o.end_date IS NULL
        ORDER BY o.start_date
        """
    )
    occs = c.fetchall()
    conn.close()
    return occs


def compute_billing(year: int, month: int):
    """
    Compute billing for the given year and month. Returns a dictionary keyed by
    tenant_id containing tenant name, items (unit_name, days, daily_rate, charge)
    and total amount.
    """
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    first_day = date(year, month, 1)
    last_day = date(year, month, monthrange(year, month)[1])
    # Occupancies overlapping the month
    c.execute(
        """
        SELECT o.id, o.unit_id, o.tenant_id, o.start_date, o.end_date, o.daily_rate,
               u.name AS unit_name, t.name AS tenant_name
        FROM occupancies o
        JOIN units u ON o.unit_id = u.id
        JOIN tenants t ON o.tenant_id = t.id
        WHERE (
            (DATE(o.start_date) <= DATE(?) AND (o.end_date IS NULL OR DATE(o.end_date) >= DATE(?))) OR
            (DATE(o.start_date) BETWEEN DATE(?) AND DATE(?)) OR
            (o.end_date IS NOT NULL AND DATE(o.end_date) BETWEEN DATE(?) AND DATE(?))
        )
        """,
        (
            last_day.isoformat(), first_day.isoformat(),
            first_day.isoformat(), last_day.isoformat(),
            first_day.isoformat(), last_day.isoformat(),
        ),
    )
    records = c.fetchall()
    conn.close()
    billing = {}
    for rec in records:
        occ_start = datetime.strptime(rec["start_date"], "%Y-%m-%d").date()
        occ_end = datetime.strptime(rec["end_date"], "%Y-%m-%d").date() if rec["end_date"] else None
        start = max(occ_start, first_day)
        end = min(occ_end if occ_end else last_day, last_day)
        days = (end - start).days + 1
        charge = days * rec["daily_rate"]
        tenant_id = rec["tenant_id"]
        tenant_name = rec["tenant_name"]
        if tenant_id not in billing:
            billing[tenant_id] = {"tenant_name": tenant_name, "items": [], "total": 0.0}
        billing[tenant_id]["items"].append(
            {
                "unit_name": rec["unit_name"],
                "days": days,
                "daily_rate": rec["daily_rate"],
                "charge": charge,
            }
        )
        billing[tenant_id]["total"] += charge
    return billing


def create_invoices(year: int, month: int):
    """
    Generate invoices for all tenants for a given period. If an invoice already
    exists for a tenant, it will be updated. Returns a list of invoice IDs
    created or updated.
    """
    billing_data = compute_billing(year, month)
    conn = get_db_connection()
    c = conn.cursor()
    now_iso = datetime.utcnow().isoformat()
    invoice_ids = []
    for tenant_id, data in billing_data.items():
        total = data["total"]
        # Check if invoice exists
        c.execute(
            "SELECT id FROM invoices WHERE tenant_id = ? AND year = ? AND month = ?",
            (tenant_id, year, month),
        )
        row = c.fetchone()
        if row:
            invoice_id = row[0]
            # Remove previous items
            c.execute("DELETE FROM invoice_items WHERE invoice_id = ?", (invoice_id,))
            # Update invoice header
            c.execute(
                "UPDATE invoices SET total_amount = ?, status = 'pending', created_at = ?, paid_at = NULL WHERE id = ?",
                (total, now_iso, invoice_id),
            )
        else:
            c.execute(
                "INSERT INTO invoices (tenant_id, year, month, total_amount, status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)",
                (tenant_id, year, month, total, now_iso),
            )
            invoice_id = c.lastrowid
        # Insert items
        for item in data["items"]:
            c.execute(
                "INSERT INTO invoice_items (invoice_id, unit_name, days, daily_rate, amount) VALUES (?, ?, ?, ?, ?)",
                (invoice_id, item["unit_name"], item["days"], item["daily_rate"], item["charge"]),
            )
        invoice_ids.append(invoice_id)
    conn.commit()
    conn.close()
    return invoice_ids


def fetch_invoice(invoice_id: int):
    """Retrieve an invoice and its items."""
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(
        "SELECT i.*, t.name AS tenant_name, t.email AS tenant_email, t.phone AS tenant_phone FROM invoices i JOIN tenants t ON i.tenant_id = t.id WHERE i.id = ?",
        (invoice_id,),
    )
    invoice = c.fetchone()
    if not invoice:
        conn.close()
        return None, []
    c.execute("SELECT * FROM invoice_items WHERE invoice_id = ?", (invoice_id,))
    items = c.fetchall()
    conn.close()
    return invoice, items


def generate_invoice_pdf(invoice_id: int):
    """
    Generate a simple PDF invoice using matplotlib. Returns the path to the file.
    """
    invoice, items = fetch_invoice(invoice_id)
    if not invoice:
        return None
    tenant_name = invoice["tenant_name"]
    period = f"{date(1900, invoice['month'], 1).strftime('%B')} {invoice['year']}"
    total = invoice["total_amount"]
    lines = []
    lines.append(f"Invoice ID: {invoice_id}")
    lines.append(f"Tenant: {tenant_name}")
    lines.append(f"Period: {period}")
    lines.append("")
    lines.append("Details:")
    for item in items:
        unit = item["unit_name"]
        days = item["days"]
        rate = item["daily_rate"]
        amt = item["amount"]
        lines.append(f" - Unit {unit}: {days} day(s) × ₱{rate:.2f} = ₱{amt:.2f}")
    lines.append("")
    lines.append(f"Total Amount Due: ₱{total:.2f}")
    # Ensure output directory exists
    invoices_dir = os.path.join(os.path.dirname(__file__), "generated_invoices")
    os.makedirs(invoices_dir, exist_ok=True)
    file_path = os.path.join(invoices_dir, f"invoice_{invoice_id}.pdf")
    with PdfPages(file_path) as pdf:
        fig, ax = plt.subplots(figsize=(8.27, 11.69))
        ax.axis('off')
        y = 0.95
        for line in lines:
            ax.text(0.1, y, line, fontsize=12, transform=ax.transAxes)
            y -= 0.05
        pdf.savefig(fig)
    return file_path


def get_current_user(request: Request):
    """Return the currently authenticated user from the session cookie, or None."""
    token = request.cookies.get("session_token")
    if not token:
        return None
    session = sessions.get(token)
    if not session:
        return None
    if session["expiry"] <= datetime.utcnow():
        sessions.pop(token, None)
        return None
    return session["user"]


# Set up FastAPI application
app = FastAPI()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="static",
)

# Initialise database and create default admin
init_db()
create_default_admin()


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    units = fetch_units()
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "units": units, "user": user},
    )


@app.get("/units", response_class=HTMLResponse)
async def list_units(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    units = fetch_units()
    return templates.TemplateResponse(
        "units.html",
        {"request": request, "units": units, "user": user},
    )


@app.get("/units/add", response_class=HTMLResponse)
async def add_unit_form(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("add_unit.html", {"request": request, "user": user})


@app.post("/units/add")
async def add_unit_submit(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    body = await request.body()
    from urllib.parse import parse_qs
    data = parse_qs(body.decode())
    name = data.get("name", [""])[0].strip()
    daily_rate_str = data.get("daily_rate", ["0"])[0]
    try:
        daily_rate = float(daily_rate_str)
    except ValueError:
        daily_rate = 0.0
    add_unit(name, daily_rate)
    return RedirectResponse(url="/units", status_code=303)


@app.get("/units/edit/{unit_id}", response_class=HTMLResponse)
async def edit_unit_form(request: Request, unit_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM units WHERE id = ?", (unit_id,))
    unit = c.fetchone()
    conn.close()
    return templates.TemplateResponse(
        "edit_unit.html",
        {"request": request, "unit": unit, "user": user},
    )


@app.post("/units/edit/{unit_id}")
async def edit_unit_submit(request: Request, unit_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    body = await request.body()
    from urllib.parse import parse_qs
    data = parse_qs(body.decode())
    name = data.get("name", [""])[0].strip()
    daily_rate_str = data.get("daily_rate", ["0"])[0]
    try:
        daily_rate = float(daily_rate_str)
    except ValueError:
        daily_rate = 0.0
    update_unit_rate(unit_id, daily_rate)
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE units SET name = ? WHERE id = ?", (name, unit_id))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/units", status_code=303)


@app.get("/tenants", response_class=HTMLResponse)
async def list_tenants(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    tenants = fetch_tenants()
    return templates.TemplateResponse(
        "tenants.html",
        {"request": request, "tenants": tenants, "user": user},
    )


@app.get("/tenants/add", response_class=HTMLResponse)
async def add_tenant_form(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("add_tenant.html", {"request": request, "user": user})


@app.post("/tenants/add")
async def add_tenant_submit(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    body = await request.body()
    from urllib.parse import parse_qs
    data = parse_qs(body.decode())
    name = data.get("name", [""])[0].strip()
    email = data.get("email", [None])[0]
    phone = data.get("phone", [None])[0]
    if email is not None:
        email = email.strip()
    if phone is not None:
        phone = phone.strip()
    add_tenant(name, email, phone)
    return RedirectResponse(url="/tenants", status_code=303)


@app.get("/occupancies", response_class=HTMLResponse)
async def list_occupancies(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    occupancies = fetch_active_occupancies()
    return templates.TemplateResponse(
        "occupancies.html",
        {"request": request, "occupancies": occupancies, "user": user},
    )


@app.get("/occupancies/add", response_class=HTMLResponse)
async def add_occupancy_form(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    # Show only units that are vacant
    units = [u for u in fetch_units() if u["occupancy_id"] is None]
    tenants = fetch_tenants()
    today_str = date.today().isoformat()
    preselected_unit = request.query_params.get("unit_id")
    return templates.TemplateResponse(
        "add_occupancy.html",
        {
            "request": request,
            "units": units,
            "tenants": tenants,
            "today": today_str,
            "preselected_unit": int(preselected_unit) if preselected_unit else None,
            "user": user,
        },
    )


@app.post("/occupancies/add")
async def add_occupancy_submit(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    body = await request.body()
    from urllib.parse import parse_qs
    data = parse_qs(body.decode())
    unit_id_str = data.get("unit_id", ["0"])[0]
    tenant_id_str = data.get("tenant_id", ["0"])[0]
    start_date_str = data.get("start_date", [date.today().isoformat()])[0]
    try:
        unit_id = int(unit_id_str)
        tenant_id = int(tenant_id_str)
    except ValueError:
        return RedirectResponse(url="/occupancies", status_code=303)
    try:
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    except ValueError:
        start_dt = date.today()
    assign_unit(unit_id, tenant_id, start_dt)
    return RedirectResponse(url="/occupancies", status_code=303)


@app.get("/occupancies/end/{occupancy_id}")
async def end_occupancy_endpoint(request: Request, occupancy_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    today_dt = date.today()
    end_occupancy(occupancy_id, today_dt)
    return RedirectResponse(url="/occupancies", status_code=303)


@app.get("/billing", response_class=HTMLResponse)
async def view_billing(request: Request, year: int = None, month: int = None):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    today_date = date.today()
    billing_year = year if year else today_date.year
    billing_month = month if month else today_date.month
    billing_data = compute_billing(billing_year, billing_month)
    months_list = [
        {"value": i, "name": date(1900, i, 1).strftime("%B")}
        for i in range(1, 13)
    ]
    return templates.TemplateResponse(
        "billing.html",
        {
            "request": request,
            "billing_data": billing_data,
            "selected_year": billing_year,
            "selected_month": billing_month,
            "months": months_list,
            "user": user,
        },
    )


# Authentication routes
@app.get("/login", response_class=HTMLResponse)
async def login_form(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "user": None})


@app.post("/login")
async def login_submit(request: Request):
    body = await request.body()
    from urllib.parse import parse_qs
    data = parse_qs(body.decode())
    username = data.get("username", [""])[0].strip()
    password = data.get("password", [""])[0]
    user = get_user_by_username(username)
    if user and verify_password(password, user["password_hash"]):
        token = secrets.token_hex(16)
        expiry = datetime.utcnow() + timedelta(hours=12)
        sessions[token] = {"user": dict(user), "expiry": expiry}
        add_login_record(user["id"])
        response = RedirectResponse(url="/", status_code=303)
        response.set_cookie(key="session_token", value=token, httponly=True)
        return response
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": "Invalid username or password", "user": None},
        status_code=401,
    )


@app.get("/logout")
async def logout(request: Request):
    token = request.cookies.get("session_token")
    if token and token in sessions:
        sessions.pop(token, None)
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie("session_token")
    return response


# Invoice routes
@app.get("/invoices", response_class=HTMLResponse)
async def list_invoices(request: Request, year: int = None, month: int = None):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    selected_year = year
    selected_month = month
    if year and month:
        c.execute(
            """
            SELECT i.*, t.name AS tenant_name
            FROM invoices i
            JOIN tenants t ON i.tenant_id = t.id
            WHERE i.year = ? AND i.month = ?
            ORDER BY t.name
            """,
            (year, month),
        )
    else:
        c.execute(
            """
            SELECT i.*, t.name AS tenant_name
            FROM invoices i
            JOIN tenants t ON i.tenant_id = t.id
            ORDER BY i.year DESC, i.month DESC, t.name
            """
        )
    rows = c.fetchall()
    conn.close()
    # Enrich invoices with period and due flag
    today_date = date.today()
    invoices = []
    for row in rows:
        inv = dict(row)
        inv["period"] = date(1900, inv["month"], 1).strftime("%B %Y")
        created_date = datetime.fromisoformat(inv["created_at"]).date()
        due_date = created_date + timedelta(days=30)
        inv["due_date"] = due_date
        inv["is_due"] = inv["status"] != "paid" and today_date > due_date
        invoices.append(inv)
    months_list = [
        {"value": i, "name": date(1900, i, 1).strftime("%B")}
        for i in range(1, 13)
    ]
    return templates.TemplateResponse(
        "invoices.html",
        {
            "request": request,
            "invoices": invoices,
            "selected_year": selected_year,
            "selected_month": selected_month,
            "months": months_list,
            "user": user,
        },
    )


@app.get("/invoices/generate")
async def generate_invoices_endpoint(request: Request, year: int = None, month: int = None):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    today_date = date.today()
    gen_year = year if year else today_date.year
    gen_month = month if month else today_date.month
    create_invoices(gen_year, gen_month)
    return RedirectResponse(url=f"/invoices?year={gen_year}&month={gen_month}", status_code=303)


@app.get("/invoice/{invoice_id}", response_class=HTMLResponse)
async def invoice_detail(request: Request, invoice_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    invoice, items = fetch_invoice(invoice_id)
    if not invoice:
        return HTMLResponse(content="Invoice not found", status_code=404)
    created_date = datetime.fromisoformat(invoice["created_at"]).date()
    due_date = created_date + timedelta(days=30)
    is_due = invoice["status"] != "paid" and date.today() > due_date
    # Format period string for display
    period_str = date(1900, invoice["month"], 1).strftime("%B %Y")
    return templates.TemplateResponse(
        "invoice_detail.html",
        {
            "request": request,
            "invoice": invoice,
            "items": items,
            "due_date": due_date,
            "is_due": is_due,
            "period": period_str,
            "user": user,
        },
    )


@app.get("/invoice/{invoice_id}/pdf")
async def invoice_pdf_endpoint(request: Request, invoice_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    path = generate_invoice_pdf(invoice_id)
    if not path:
        return HTMLResponse(content="Invoice not found", status_code=404)
    filename = os.path.basename(path)
    return FileResponse(path=path, filename=filename, media_type="application/pdf")


@app.get("/invoice/{invoice_id}/pay")
async def pay_invoice_endpoint(request: Request, invoice_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    conn = get_db_connection()
    c = conn.cursor()
    now_iso = datetime.utcnow().isoformat()
    c.execute(
        "UPDATE invoices SET status = 'paid', paid_at = ? WHERE id = ?",
        (now_iso, invoice_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/invoice/{invoice_id}", status_code=303)