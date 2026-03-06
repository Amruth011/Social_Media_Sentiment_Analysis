import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# ----------------- Configuration -----------------
st.set_page_config(
    page_title="Social Sentiment Engine | Executive Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for UI Polish
st.markdown("""
<style>
    .metric-card {
        background-color: #1e1e1e;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        text-align: center;
        margin-bottom: 20px;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #00A4E4;
    }
    .metric-label {
        font-size: 1rem;
        color: #a0a0a0;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
</style>
""", unsafe_allow_html=True)

# ----------------- Data Loading (Simulation of Delta Lake Read) -----------------
@st.cache_data
def load_data():
    """
    Simulates reading from the Delta Lake Bronze/Silver layers.
    In a live Databricks environment, this would be: 
    df = spark.read.format("delta").load("s3://bucket/data/output/...")
    """
    csv_path = "SocialMediaSentimentAnalysiscsv.csv"
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        # Clean data (mocking the PySpark .na.drop() and dropDuplicates())
        df = df.dropna().drop_duplicates()
        
        # Mocking PySpark UDF: Text Length Category
        def categorize_length(text):
            if not isinstance(text, str): return "Unknown"
            length = len(text)
            if length < 50: return "Short"
            elif length < 100: return "Medium"
            else: return "Long"
            
        df['Text_Length_Category'] = df['Text'].apply(categorize_length)
        return df
    else:
        # Fallback dummy data for presentation if CSV is missing
        return pd.DataFrame({
            'Platform': ['Twitter', 'Instagram', 'Facebook', 'Twitter', 'Facebook'],
            'Sentiment': ['Positive', 'Positive', 'Negative', 'Neutral', 'Positive'],
            'Likes': [150, 400, 20, 10, 800],
            'Retweets': [20, 50, 2, 1, 100],
            'Country': ['USA', 'UK', 'USA', 'India', 'India'],
            'Hour': [12, 14, 2, 8, 18],
            'Text_Length_Category': ['Short', 'Medium', 'Short', 'Long', 'Medium']
        })

df = load_data()

# ----------------- Sidebar -----------------
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/f/f3/Apache_Spark_logo.svg", width=150)
    st.title("Data Engineering Engine")
    st.markdown("---")
    st.markdown("""
    **Architecture Info**
    - **Engine:** PySpark (Batch)
    - **Storage:** Delta Lake (ACID)
    - **Deployment:** Docker / GitHub Actions
    """)
    st.markdown("---")
    platform_filter = st.selectbox("Filter Platform", options=["All"] + list(df['Platform'].unique()))
    sentiment_filter = st.selectbox("Filter Sentiment", options=["All"] + list(df['Sentiment'].unique()))

# Filter Data
filtered_df = df.copy()
if platform_filter != "All":
    filtered_df = filtered_df[filtered_df['Platform'] == platform_filter]
if sentiment_filter != "All":
    filtered_df = filtered_df[filtered_df['Sentiment'] == sentiment_filter]

# ----------------- Main Dashboard -----------------
st.title("📊 Social Media Sentiment Lakehouse")
st.markdown("*Live Executive Dashboard rendering PySpark enriched data streams.*")

# Row 1: KPI Metrics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f'<div class="metric-card"><div class="metric-value">{len(filtered_df):,}</div><div class="metric-label">Total Records Processed</div></div>', unsafe_allow_html=True)
with col2:
    avg_likes = int(filtered_df['Likes'].mean()) if not filtered_df.empty else 0
    st.markdown(f'<div class="metric-card"><div class="metric-value">{avg_likes:,}</div><div class="metric-label">Avg. Likes / Post</div></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="metric-card"><div class="metric-value">Delta Lake</div><div class="metric-label">Storage Format</div></div>', unsafe_allow_html=True)
with col4:
    active_countries = filtered_df['Country'].nunique()
    st.markdown(f'<div class="metric-card"><div class="metric-value">{active_countries}</div><div class="metric-label">Countries Engaged</div></div>', unsafe_allow_html=True)

st.markdown("---")

# Row 2: Charts
tab1, tab2, tab3 = st.tabs(["🌎 Geographic Insights", "📱 Platform Analytics", "🕒 Time-Series Trends"])

with tab1:
    col_a, col_b = st.columns([2, 1])
    with col_a:
        st.subheader("Global Engagement Topologies")
        country_agg = filtered_df.groupby('Country', as_index=False)[['Likes', 'Retweets']].sum()
        fig_map = px.choropleth(country_agg, locations='Country', locationmode='country names', 
                                color='Likes', hover_name='Country', 
                                color_continuous_scale=px.colors.sequential.Teal,
                                title='Total Engagement (Likes) by Geography')
        fig_map.update_layout(geo=dict(showframe=False, showcoastlines=False), margin={"r":0,"t":40,"l":0,"b":0})
        st.plotly_chart(fig_map, use_container_width=True)
    with col_b:
        st.subheader("Sentiment Distribution")
        sent_agg = filtered_df['Sentiment'].value_counts().reset_index()
        sent_agg.columns = ['Sentiment', 'Count']
        fig_pie = px.pie(sent_agg, names='Sentiment', values='Count', hole=0.4, 
                         color='Sentiment', color_discrete_map={'Positive': '#2ca02c', 'Negative': '#d62728', 'Neutral': '#7f7f7f'},
                         title='Overall Sentiment Split')
        st.plotly_chart(fig_pie, use_container_width=True)

with tab2:
    col_c, col_d = st.columns(2)
    with col_c:
        st.subheader("Platform Dominance")
        platform_agg = filtered_df.groupby('Platform', as_index=False)['Likes'].sum()
        fig_bar = px.bar(platform_agg, x='Platform', y='Likes', color='Platform', 
                         title='Total Likes by Platform', text_auto='.2s')
        st.plotly_chart(fig_bar, use_container_width=True)
    with col_d:
        st.subheader("PySpark Feature Engineering (UDF)")
        len_agg = filtered_df.groupby(['Platform', 'Text_Length_Category'], as_index=False).size()
        fig_stack = px.bar(len_agg, x='Platform', y='size', color='Text_Length_Category', 
                           title='Tweet Length Categories (Engineered via UDF)')
        st.plotly_chart(fig_stack, use_container_width=True)

with tab3:
    st.subheader("Hourly Sentiment Velocity")
    time_agg = filtered_df.groupby(['Hour', 'Sentiment'], as_index=False).size()
    fig_line = px.line(time_agg, x='Hour', y='size', color='Sentiment', markers=True, 
                       title='Volume of Sentiments Throughout the Day (24H)',
                       color_discrete_map={'Positive': '#2ca02c', 'Negative': '#d62728', 'Neutral': '#7f7f7f'})
    st.plotly_chart(fig_line, use_container_width=True)

# ----------------- Footer -----------------
st.markdown("---")
st.markdown("<div align='center'>Built with ❤️ using PySpark, Delta Lake, Streamlit, and Plotly. Designed for Enterprise Data Architecture.</div>", unsafe_allow_html=True)
