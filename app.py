import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from fpdf import FPDF
import io

# --- CONFIGURATION ---
DB_FILE = "warehouse_system.db"
st.set_page_config(page_title="Warehouse Manager", layout="wide")

# --- DATABASE SETUP ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # UNITS: Tracks physical rooms and current market rates
    c.execute('''CREATE TABLE IF NOT EXISTS units (
                    unit_id TEXT PRIMARY KEY, 
                    size TEXT, 
                    current_rate REAL, 
                    status TEXT DEFAULT 'Vacant')''')
    
    # LEASES: The historical record. Rate is frozen here at creation.
    c.execute('''CREATE TABLE IF NOT EXISTS leases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    lessee_name TEXT, 
                    lessee_email TEXT,
                    unit_id TEXT, 
                    start_date DATE, 
                    end_date DATE, 
                    frozen_rate REAL,
                    is_active BOOLEAN DEFAULT 1)''')
    
    # PAYMENTS: Tracks generated bills
    c.execute('''CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    lessee_name TEXT, 
                    billing_month TEXT,
                    total_amount REAL, 
                    status TEXT DEFAULT 'Unpaid',
                    generated_date DATE)''')
    conn.commit()
    conn.close()

init_db()

# --- HELPER FUNCTIONS ---
def get_db_connection():
    return sqlite3.connect(DB_FILE)

def calculate_days_in_month(start_date, end_date, billing_month_start):
    # Determine the end of the billing month
    billing_month_end = billing_month_start + relativedelta(months=1, days=-1)
    
    # Handle open-ended leases
    effective_end = end_date if end_date else billing_month_end
    
    # Calculate overlap
    actual_start = max(start_date, billing_month_start)
    actual_end = min(effective_end, billing_month_end)
    
    delta = (actual_end - actual_start).days + 1
    return max(0, delta)

def generate_pdf(invoice_data, billing_period):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    
    pdf.cell(200, 10, txt="WAREHOUSE INVOICE", ln=True, align='C')
    pdf.cell(200, 10, txt=f"Billing Period: {billing_period}", ln=True, align='C')
    pdf.ln(10)
    
    pdf.cell(200, 10, txt=f"Bill To: {invoice_data['lessee']}", ln=True)
    pdf.ln(5)
    
    # Table Header
    pdf.set_font("Arial", 'B', 10)
    pdf.cell(40, 10, "Unit", 1)
    pdf.cell(30, 10, "Days", 1)
    pdf.cell(40, 10, "Rate/Day", 1)
    pdf.cell(40, 10, "Subtotal", 1)
    pdf.ln()
    
    # Table Body
    pdf.set_font("Arial", size=10)
    total = 0
    for item in invoice_data['items']:
        pdf.cell(40, 10, str(item['unit']), 1)
        pdf.cell(30, 10, str(item['days']), 1)
        pdf.cell(40, 10, f"${item['rate']:.2f}", 1)
        pdf.cell(40, 10, f"${item['total']:.2f}", 1)
        pdf.ln()
        total += item['total']
        
    pdf.ln(5)
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(150, 10, "TOTAL DUE:", 0, 0, 'R')
    pdf.cell(40, 10, f"${total:.2f}", 0, 1, 'R')
    
    return pdf.output(dest='S').encode('latin-1')

# --- SIDEBAR NAV ---
st.sidebar.title("📦 Storage Manager")
page = st.sidebar.radio("Navigate", ["Dashboard", "Units & Rates", "Lease Management", "Billing Center", "Payment History"])

# --- PAGE 1: DASHBOARD ---
if page == "Dashboard":
    st.title("Facility Overview")
    
    conn = get_db_connection()
    
    # 1. Metrics
    total_units = pd.read_sql("SELECT COUNT(*) FROM units", conn).iloc[0,0]
    occupied_units = pd.read_sql("SELECT COUNT(*) FROM units WHERE status='Occupied'", conn).iloc[0,0]
    vacant_units = total_units - occupied_units
    
    overdue_bills = pd.read_sql("SELECT COUNT(*) FROM payments WHERE status='Unpaid'", conn).iloc[0,0]
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Occupancy", f"{occupied_units}/{total_units}")
    col2.metric("Vacancy", vacant_units)
    col3.metric("Unpaid Invoices", overdue_bills, delta_color="inverse")
    
    st.divider()
    
    # 2. Alerts Section
    st.subheader("⚠️ Alerts")
    
    # Expiring Leases Logic
    today = date.today()
    next_week = today + timedelta(days=7)
    expiring = pd.read_sql(f"SELECT lessee_name, unit_id, end_date FROM leases WHERE end_date BETWEEN '{today}' AND '{next_week}' AND is_active=1", conn)
    
    if not expiring.empty:
        st.warning(f"You have {len(expiring)} leases expiring in the next 7 days!")
        st.dataframe(expiring)
    else:
        st.success("No leases expiring this week.")

    # 3. Visual Grid
    st.subheader("Unit Map")
    units = pd.read_sql("SELECT * FROM units ORDER BY unit_id", conn)
    
    if not units.empty:
        # Create a grid layout
        cols = st.columns(5)
        for index, row in units.iterrows():
            with cols[index % 5]:
                color = "🔴" if row['status'] == 'Occupied' else "🟢"
                st.button(f"{color} {row['unit_id']}\n${row['current_rate']}/day", key=row['unit_id'], use_container_width=True)
                
    conn.close()

# --- PAGE 2: UNITS & RATES ---
elif page == "Units & Rates":
    st.title("Manage Inventory")
    
    with st.expander("Add New Unit"):
        with st.form("add_unit"):
            u_id = st.text_input("Unit ID (e.g., A-101)")
            u_size = st.text_input("Size (e.g., 10x10)")
            u_rate = st.number_input("Daily Rate", min_value=0.0, format="%.2f")
            submitted = st.form_submit_button("Add Unit")
            if submitted:
                conn = get_db_connection()
                try:
                    conn.execute("INSERT INTO units (unit_id, size, current_rate) VALUES (?,?,?)", (u_id, u_size, u_rate))
                    conn.commit()
                    st.success(f"Unit {u_id} added.")
                except:
                    st.error("Unit ID already exists.")
                conn.close()

    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM units", conn)
    edited_df = st.data_editor(df, num_rows="dynamic", key="editor")
    
    if st.button("Save Changes to Rates"):
        # This allows you to update rates easily via the grid
        for i, row in edited_df.iterrows():
            conn.execute("UPDATE units SET current_rate=?, size=? WHERE unit_id=?", (row['current_rate'], row['size'], row['unit_id']))
        conn.commit()
        st.success("Rates updated! Note: This only affects NEW leases.")
    conn.close()

# --- PAGE 3: LEASE MANAGEMENT ---
elif page == "Lease Management":
    st.title("Check-In / Check-Out")
    
    tab1, tab2 = st.tabs(["New Lease (Check-In)", "Active Leases"])
    
    conn = get_db_connection()
    
    with tab1:
        st.subheader("Register New Tenant")
        vacant_units = pd.read_sql("SELECT unit_id, current_rate FROM units WHERE status='Vacant'", conn)
        
        with st.form("new_lease"):
            col_a, col_b = st.columns(2)
            name = col_a.text_input("Lessee Name")
            email = col_b.text_input("Email")
            
            unit_choice = st.selectbox("Select Unit", vacant_units['unit_id'] if not vacant_units.empty else [])
            start_dt = st.date_input("Start Date", value=date.today())
            
            if st.form_submit_button("Create Lease"):
                if unit_choice:
                    # Get current rate to FREEZE it
                    rate = vacant_units.loc[vacant_units['unit_id'] == unit_choice, 'current_rate'].values[0]
                    
                    conn.execute("INSERT INTO leases (lessee_name, lessee_email, unit_id, start_date, frozen_rate) VALUES (?,?,?,?,?)",
                                 (name, email, unit_choice, start_dt, rate))
                    conn.execute("UPDATE units SET status='Occupied' WHERE unit_id=?", (unit_choice,))
                    conn.commit()
                    st.success(f"Lease created for {name} in {unit_choice} at ${rate}/day")
                    st.rerun()
                else:
                    st.error("No vacant units available.")

    with tab2:
        st.subheader("Manage Active Leases")
        active = pd.read_sql("SELECT id, lessee_name, unit_id, start_date, frozen_rate FROM leases WHERE is_active=1", conn)
        
        for index, row in active.iterrows():
            with st.expander(f"{row['unit_id']} - {row['lessee_name']}"):
                col1, col2 = st.columns(2)
                col1.write(f"Start Date: {row['start_date']}")
                col1.write(f"Rate: ${row['frozen_rate']}/day")
                
                if col2.button("End Lease (Check-Out)", key=f"end_{row['id']}"):
                    conn.execute("UPDATE leases SET is_active=0, end_date=? WHERE id=?", (date.today(), row['id']))
                    conn.execute("UPDATE units SET status='Vacant' WHERE unit_id=?", (row['unit_id'],))
                    conn.commit()
                    st.success("Lease ended.")
                    st.rerun()
    conn.close()

# --- PAGE 4: BILLING CENTER ---
elif page == "Billing Center":
    st.title("Monthly Billing Generator")
    
    # Select Month
    col1, col2 = st.columns(2)
    bill_year = col1.number_input("Year", value=date.today().year)
    bill_month = col2.selectbox("Month", range(1, 13), index=date.today().month - 1)
    
    if st.button("Generate Billing Preview"):
        start_of_month = date(bill_year, bill_month, 1)
        end_of_month = start_of_month + relativedelta(months=1, days=-1)
        
        conn = get_db_connection()
        # Get all leases that overlap with this month
        leases = pd.read_sql(f"""
            SELECT * FROM leases 
            WHERE start_date <= '{end_of_month}' 
            AND (end_date >= '{start_of_month}' OR end_date IS NULL)
        """, conn)
        
        # Process Billing
        invoices = {} # Key: Lessee Name, Value: Data
        
        for _, lease in leases.iterrows():
            l_start = datetime.strptime(lease['start_date'], '%Y-%m-%d').date()
            l_end = datetime.strptime(lease['end_date'], '%Y-%m-%d').date() if lease['end_date'] else None
            
            days = calculate_days_in_month(l_start, l_end, start_of_month)
            cost = days * lease['frozen_rate']
            
            if days > 0:
                name = lease['lessee_name']
                if name not in invoices:
                    invoices[name] = {'total': 0, 'items': [], 'email': lease['lessee_email']}
                
                invoices[name]['items'].append({
                    'unit': lease['unit_id'],
                    'days': days,
                    'rate': lease['frozen_rate'],
                    'total': cost
                })
                invoices[name]['total'] += cost
        
        # Display Results
        if not invoices:
            st.info("No billable activity found for this period.")
        else:
            st.write(f"Found {len(invoices)} billable lessees.")
            
            for name, data in invoices.items():
                with st.container():
                    st.markdown(f"### {name}")
                    st.markdown(f"**Total Due: ${data['total']:.2f}**")
                    
                    df_bill = pd.DataFrame(data['items'])
                    st.dataframe(df_bill)
                    
                    # PDF Generation
                    pdf_bytes = generate_pdf({'lessee': name, 'items': data['items']}, start_of_month.strftime('%B %Y'))
                    
                    col_a, col_b = st.columns([1, 4])
                    col_a.download_button(
                        label="📄 Download PDF",
                        data=pdf_bytes,
                        file_name=f"Invoice_{name}_{start_of_month}.pdf",
                        mime='application/pdf'
                    )
                    
                    if col_b.button(f"Record as Unpaid in Ledger", key=f"rec_{name}"):
                        conn.execute("INSERT INTO payments (lessee_name, billing_month, total_amount, status, generated_date) VALUES (?,?,?,?,?)",
                                     (name, start_of_month.strftime('%Y-%m'), data['total'], 'Unpaid', date.today()))
                        conn.commit()
                        st.success("Recorded to Payment History")
                    st.divider()
        conn.close()

# --- PAGE 5: PAYMENT HISTORY ---
elif page == "Payment History":
    st.title("Payment Ledger")
    
    conn = get_db_connection()
    payments = pd.read_sql("SELECT * FROM payments ORDER BY generated_date DESC", conn)
    
    # Edit Status
    edited_payments = st.data_editor(
        payments, 
        column_config={
            "status": st.column_config.SelectboxColumn(
                "Status",
                options=["Unpaid", "Paid", "Cancelled"],
                required=True
            )
        },
        key="payment_editor"
    )
    
    if st.button("Update Payment Statuses"):
        for i, row in edited_payments.iterrows():
            # Update DB only if changed (simplified logic updates all)
            conn.execute("UPDATE payments SET status=? WHERE id=?", (row['status'], row['id']))
        conn.commit()
        st.success("Ledger Updated")
    
    # Financial Stats
    st.divider()
    paid_total = payments[payments['status']=='Paid']['total_amount'].sum()
    unpaid_total = payments[payments['status']=='Unpaid']['total_amount'].sum()
    
    c1, c2 = st.columns(2)
    c1.metric("Total Collected", f"${paid_total:,.2f}")
    c2.metric("Pending Collections", f"${unpaid_total:,.2f}", delta_color="inverse")
    
    conn.close()

