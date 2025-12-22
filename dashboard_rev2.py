import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account

# --- 1. PAGE CONFIGURATION ---
st.set_page_config(page_title="Executive Command Center", layout="wide", initial_sidebar_state="collapsed")

# --- CUSTOM CSS FOR "PRESENTATION MODE" (LARGE FONTS) ---
st.markdown("""
    <style>
    /* 1. Global Text Size */
    html, body, [class*="css"]  {
        font-family: 'Sans Serif';
    }
    
    /* 2. TAB TITLES (The clickable buttons) */
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 24px !important;
        font-weight: bold !important;
    }
    
    /* 3. METRIC CARDS (Top Row) */
    [data-testid="stMetricValue"] {
        font-size: 40px !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 18px !important;
        font-weight: bold !important;
    }

    /* 4. HEADERS */
    h1 { font-size: 48px !important; }
    h2 { font-size: 36px !important; }
    h3 { font-size: 28px !important; }
    
    /* 5. NORMAL TEXT */
    .stMarkdown p {
        font-size: 18px !important;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🇧🇷 Brazilian E-Commerce Strategic Dashboard")
st.markdown("### 🚀 Module 2 Project: High-Level Business Intelligence")

# --- 2. AUTHENTICATION ---
KEY_FILE = '/Users/govindandhanasekaran/Dev/Github/stellar-verve-478012-n6-5c79fd657d1a.json'
PROJECT_ID = 'stellar-verve-478012-n6'
DATASET = 'olist_raw_analytics'


@st.cache_resource
def get_client():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            KEY_FILE, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    except Exception as e:
        st.error(f"🚨 Connection Error: {e}")
        return None

@st.cache_data
def run_query(query):
    client = get_client()
    if client:
        return client.query(query).to_dataframe()
    return pd.DataFrame()

# --- 3. DATA INGESTION ---
with st.spinner('Crunching BigQuery Data...'):
    
    # KPI Query
    sql_kpi = f"""
        SELECT 
            (SELECT COUNT(DISTINCT order_id) FROM `{PROJECT_ID}.{DATASET}.fct_orders`) as total_orders,
            (SELECT SUM(payment_value) FROM `{PROJECT_ID}.{DATASET}.dim_payments`) as total_revenue,
            (SELECT AVG(review_score) FROM `{PROJECT_ID}.{DATASET}.dim_reviews`) as avg_csat,
            (SELECT COUNT(DISTINCT seller_id) FROM `{PROJECT_ID}.{DATASET}.dim_sellers`) as total_sellers
    """
    df_kpi = run_query(sql_kpi)

    # 1. Monthly Revenue Trend
    sql_trend = f"""
        SELECT 
            DATE_TRUNC(DATE(o.purchase_at), MONTH) as month,
            SUM(i.price) as revenue
        FROM `{PROJECT_ID}.{DATASET}.fct_orders` o
        JOIN `{PROJECT_ID}.{DATASET}.fct_order_items` i ON o.order_id = i.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY 1 ORDER BY 1
    """
    df_trend = run_query(sql_trend)

    # 2. Top Categories
    sql_cat = f"""
        SELECT 
            p.category_name, 
            SUM(i.price) as revenue,
            COUNT(i.order_id) as volume
        FROM `{PROJECT_ID}.{DATASET}.fct_order_items` i
        JOIN `{PROJECT_ID}.{DATASET}.dim_products` p ON i.product_id = p.product_id
        WHERE p.category_name IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """
    df_cat = run_query(sql_cat)

    # 3. Delivery Speed vs Satisfaction
    sql_qual = f"""
        SELECT 
            r.review_score,
            AVG(o.time_to_delivery_hours) / 24 as avg_days
        FROM `{PROJECT_ID}.{DATASET}.dim_reviews` r
        JOIN `{PROJECT_ID}.{DATASET}.fct_orders` o ON r.order_id = o.order_id
        WHERE o.order_status = 'delivered' AND o.time_to_delivery_hours IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """
    df_qual = run_query(sql_qual)

    # 4. Payment Methods
    sql_pay = f"""
        SELECT payment_type, COUNT(*) as count 
        FROM `{PROJECT_ID}.{DATASET}.dim_payments` 
        GROUP BY 1 ORDER BY 2 DESC
    """
    df_pay = run_query(sql_pay)

    # 5. Order Distribution by Day of Week
    sql_dow = f"""
        SELECT 
            d.day_name,
            d.day_of_week, 
            COUNT(o.order_id) as orders
        FROM `{PROJECT_ID}.{DATASET}.fct_orders` o
        JOIN `{PROJECT_ID}.{DATASET}.dim_date` d ON DATE(o.purchase_at) = d.date_day
        GROUP BY 1, 2 ORDER BY 2
    """
    df_dow = run_query(sql_dow)

    # 6. Seller State Performance
    sql_seller = f"""
        SELECT s.state, SUM(i.price) as revenue
        FROM `{PROJECT_ID}.{DATASET}.dim_sellers` s
        JOIN `{PROJECT_ID}.{DATASET}.fct_order_items` i ON s.seller_id = i.seller_id
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """
    df_seller = run_query(sql_seller)

# --- 4. VISUALIZATION HELPERS (LARGE FONTS) ---
def style_plot(fig, title):
    """Applies a consistent large-font style to all Plotly charts."""
    fig.update_layout(
        title=dict(text=title, font=dict(size=24)), # Big Title
        font=dict(size=18),                         # Big General Text (Legend, Axis)
        hovermode="x unified",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    # Make axis labels specifically larger and bold
    fig.update_xaxes(title_font=dict(size=20, family='Arial', color='black'), tickfont=dict(size=16))
    fig.update_yaxes(title_font=dict(size=20, family='Arial', color='black'), tickfont=dict(size=16))
    return fig

# --- 5. DASHBOARD LAYOUT ---

# A. KPI ROW
if not df_kpi.empty:
    st.markdown("## 🎯 Key Performance Indicators")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("💰 Total Revenue", f"R$ {df_kpi['total_revenue'][0]:,.0f}")
    k2.metric("📦 Total Orders", f"{df_kpi['total_orders'][0]:,.0f}")
    k3.metric("⭐ CSAT Score", f"{df_kpi['avg_csat'][0]:.2f} / 5.0")
    k4.metric("🏪 Active Sellers", f"{df_kpi['total_sellers'][0]:,.0f}")
    st.divider()

# B. INTERACTIVE TABS
# Note: CSS above forces these tab titles to be 24px
tab1, tab2, tab3, tab4 = st.tabs(["📈 Sales Analysis", "🚚 Logistics & QA", "💳 Financials", "📅 Behavioral"])

# --- TAB 1: SALES (SIDE BY SIDE LAYOUT) ---
with tab1:
    st.markdown("### Revenue Trends & Top Products")
    
    # Create two equal columns for Side-by-Side layout
    col_sales_1, col_sales_2 = st.columns([1, 1])
    
    with col_sales_1:
        # Chart 1: Trend
        if not df_trend.empty:
            fig_trend = px.line(df_trend, x='month', y='revenue', markers=True, 
                                line_shape='spline', color_discrete_sequence=['#00CC96'])
            st.plotly_chart(style_plot(fig_trend, "Monthly Revenue Growth"), use_container_width=True)

    with col_sales_2:
        # Chart 2: Categories
        if not df_cat.empty:
            fig_cat = px.bar(df_cat, x='revenue', y='category_name', orientation='h', 
                             color='revenue', color_continuous_scale='Viridis',
                             text_auto='.2s')
            fig_cat.update_layout(yaxis={'categoryorder':'total ascending'})
            # Hiding Y-axis title because the labels are obvious
            fig_cat.update_yaxes(title=None) 
            st.plotly_chart(style_plot(fig_cat, "Top 10 Categories"), use_container_width=True)

# --- TAB 2: LOGISTICS ---
with tab2:
    col_q1, col_q2 = st.columns(2)
    
    with col_q1:
        st.markdown("### 🐢 Speed vs. Satisfaction")
        if not df_qual.empty:
            fig_qual = px.bar(df_qual, x='review_score', y='avg_days', 
                              color='avg_days', color_continuous_scale='RdYlGn_r')
            st.plotly_chart(style_plot(fig_qual, "Avg Delivery Days by Review Score"), use_container_width=True)
            st.info("💡 Note: Lower delivery time = Higher score.")

    with col_q2:
        st.markdown("### 🗺️ Seller Distribution")
        if not df_seller.empty:
            fig_seller = px.bar(df_seller, x='state', y='revenue', 
                                color='revenue', color_continuous_scale='Blues')
            st.plotly_chart(style_plot(fig_seller, "Revenue by Seller State"), use_container_width=True)

# --- TAB 3: FINANCIALS ---
with tab3:
    st.markdown("### Payment Preferences")
    if not df_pay.empty:
        c1, c2 = st.columns([2, 1])
        with c1:
            fig_pie = px.pie(df_pay, names='payment_type', values='count', hole=0.4,
                             color_discrete_sequence=px.colors.qualitative.Pastel)
            fig_pie.update_traces(textposition='inside', textinfo='percent+label', textfont_size=20)
            st.plotly_chart(style_plot(fig_pie, "Order Count by Payment Method"), use_container_width=True)
        with c2:
            st.markdown("#### 💡 Strategy Note")
            st.write("Credit Card dominance suggests a need for strong fraud detection. High Boleto usage implies a need for automated payment reminders.")

# --- TAB 4: BEHAVIORAL ---
with tab4:
    st.markdown("### When do customers buy?")
    if not df_dow.empty:
        fig_dow = px.bar(df_dow, x='day_name', y='orders', 
                         color='orders', color_continuous_scale='Magma')
        st.plotly_chart(style_plot(fig_dow, "Total Orders by Day of Week"), use_container_width=True)

st.markdown("---")
st.caption("Powered by BigQuery & Streamlit | Project: Stellar Verve")