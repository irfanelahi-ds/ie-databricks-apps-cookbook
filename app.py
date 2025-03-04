import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

st.set_page_config(layout="wide")
st.logo("assets/logo.png")
st.title("Origin - Dataset Explorer 🔍")


st.header("Datasets", divider=True)
st.subheader("Find a Dataset")
st.write(
    "Use this page to find if a particular tag data and its interval exists in our Databricks Platform"
)

cfg = Config()

@st.cache_resource
def get_connection(http_path):
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

def read_table(query, conn):
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()


tab_a, tab_b = st.tabs(["**Try it**", "**Support**"])

with tab_a:
    
    with st.form("dataset_form"):
        http_path_input = st.text_input(
            "Enter your Databricks HTTP Path:",
            value="/sql/1.0/warehouses/xxxxxx",
            placeholder="/sql/1.0/warehouses/xxxxxx"
        )
        start_date = st.date_input("Enter the start date:")
        end_date = st.date_input("Enter the end date:")
        tag_input = st.text_input("Enter tag(s) separated by commas:", placeholder="TagA, TagB")
        
        submitted = st.form_submit_button("Submit")
    
    if submitted and http_path_input and start_date and end_date and tag_input:
        
        tags = [tag.strip() for tag in tag_input.split(",")]
        tags_sql = ", ".join([f"'{tag}'" for tag in tags])
        
        
        query = f"""
        SELECT 
          tag,
          MIN(ts) AS available_start,
          MAX(ts) AS available_end,
          CASE 
            WHEN MIN(ts) <= DATE('{start_date}') 
                 AND MAX(ts) >= DATE('{end_date}') THEN 'Coverage Exists'
            ELSE 'Coverage Incomplete'
          END AS coverage_status,
          COUNT(*) AS record_count
        FROM 
          ie_ui.test_flow.aplng_pi_interpolated
        WHERE 
          tag IN ({tags_sql})
        GROUP BY 
          tag;
        """
        try:
            conn = get_connection(http_path_input)
            df = read_table(query, conn)
            
            st.dataframe(df)
            
            
            for idx, row in df.iterrows():
                tag = row["tag"]
                coverage_status = row["coverage_status"]
                available_start = row["available_start"]
                available_end = row["available_end"]
                
                if coverage_status == "Coverage Exists":
                    st.success(
                        f"**Tag {tag}**: Data exists from **{available_start}** to **{available_end}**. "
                        "You can access the data in the table [here](#)."
                    )
                else:
                    st.error(
                        f"**Tag {tag}**: Data coverage is incomplete for the requested period. "
                        "Please contact the Data Engineering team for support."
                    )
        except Exception as e:
            st.error(f"An error occurred: {e}")

with tab_b:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
                    **Dataset Info**
                    * The PI datasets exist in `ie_ui`.`aveva_pi` schema.
                    * They are updated on a daily basis.
                    """)
    with col2:
        st.markdown("""
                    **For further support**
                    * Email: [dataeng@example.com](mailto:dataeng@example.com)
                    * Slack: `#dataengineering_coe`
                    """)
