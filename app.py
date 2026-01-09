import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# Page configuration
st.set_page_config(
    page_title="Support Ticket Dashboard",
    page_icon="🎫",
    layout="wide"
)

# Constants
TABLE_NAME = "DASH_MCP_DB.DATA.FACT_SUPPORT_TICKETS"
CORTEX_SEARCH_SERVICE = "DASH_MCP_DB.DATA.SUPPORT_TICKETS_SEARCH"


@st.cache_resource
def get_session():
    """Get the active Snowflake session (Streamlit in Snowflake)."""
    return get_active_session()


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    """Execute a query and return results as DataFrame."""
    session = get_session()
    return session.sql(query).to_pandas()


@st.cache_data(ttl=300)
def get_categories() -> list:
    """Get distinct categories."""
    df = run_query(f"SELECT DISTINCT CATEGORY FROM {TABLE_NAME} ORDER BY CATEGORY")
    return ["All"] + df["CATEGORY"].tolist()


@st.cache_data(ttl=300)
def get_priorities() -> list:
    """Get distinct priorities."""
    df = run_query(f"SELECT DISTINCT PRIORITY FROM {TABLE_NAME} ORDER BY PRIORITY")
    return ["All"] + df["PRIORITY"].tolist()


@st.cache_data(ttl=300)
def get_date_range() -> tuple:
    """Get min and max dates."""
    df = run_query(f"""
        SELECT MIN(CREATED_DATE) as min_date, MAX(CREATED_DATE) as max_date
        FROM {TABLE_NAME}
    """)
    return df["MIN_DATE"].iloc[0], df["MAX_DATE"].iloc[0]


def search_tickets_cortex(query: str, limit: int = 50) -> pd.DataFrame:
    """Search tickets using Cortex Search for semantic matching."""
    session = get_session()
    escaped_query = query.replace("'", "''")

    search_query = f"""
        SELECT
            TICKET_ID,
            CUSTOMER_ID,
            CATEGORY,
            SUBCATEGORY,
            PRIORITY,
            CREATED_DATE,
            DESCRIPTION
        FROM TABLE(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                '{CORTEX_SEARCH_SERVICE}',
                '{escaped_query}',
                {{
                    'columns': ['TICKET_ID', 'CUSTOMER_ID', 'CATEGORY', 'SUBCATEGORY',
                                'PRIORITY', 'CREATED_DATE', 'DESCRIPTION'],
                    'limit': {limit}
                }}
            )
        )
    """

    try:
        return session.sql(search_query).to_pandas()
    except Exception as e:
        st.warning(f"Cortex Search not available, falling back to LIKE search: {e}")
        fallback_query = f"""
            SELECT TICKET_ID, CUSTOMER_ID, CATEGORY, SUBCATEGORY, PRIORITY, CREATED_DATE
            FROM {TABLE_NAME}
            WHERE CATEGORY ILIKE '%{escaped_query}%'
               OR SUBCATEGORY ILIKE '%{escaped_query}%'
            LIMIT {limit}
        """
        return run_query(fallback_query)


def get_filtered_tickets(category: str, priority: str, start_date, end_date, limit: int = 100) -> pd.DataFrame:
    """Get tickets with filters applied."""
    conditions = [f"CREATED_DATE BETWEEN '{start_date}' AND '{end_date}'"]

    if category != "All":
        conditions.append(f"CATEGORY = '{category}'")
    if priority != "All":
        conditions.append(f"PRIORITY = '{priority}'")

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT TICKET_ID, CUSTOMER_ID, ACCOUNT_ID, CATEGORY, SUBCATEGORY,
               PRIORITY, CREATED_DATE, GEO_ID
        FROM {TABLE_NAME}
        WHERE {where_clause}
        ORDER BY CREATED_DATE DESC
        LIMIT {limit}
    """
    return run_query(query)


def get_tickets_over_time(category: str, priority: str, start_date, end_date) -> pd.DataFrame:
    """Get ticket counts over time."""
    conditions = [f"CREATED_DATE BETWEEN '{start_date}' AND '{end_date}'"]

    if category != "All":
        conditions.append(f"CATEGORY = '{category}'")
    if priority != "All":
        conditions.append(f"PRIORITY = '{priority}'")

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT CREATED_DATE, COUNT(*) as TICKET_COUNT
        FROM {TABLE_NAME}
        WHERE {where_clause}
        GROUP BY CREATED_DATE
        ORDER BY CREATED_DATE
    """
    return run_query(query)


def get_tickets_by_category(category: str, priority: str, start_date, end_date) -> pd.DataFrame:
    """Get ticket counts by category."""
    conditions = [f"CREATED_DATE BETWEEN '{start_date}' AND '{end_date}'"]

    if category != "All":
        conditions.append(f"CATEGORY = '{category}'")
    if priority != "All":
        conditions.append(f"PRIORITY = '{priority}'")

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT CATEGORY, COUNT(*) as TICKET_COUNT
        FROM {TABLE_NAME}
        WHERE {where_clause}
        GROUP BY CATEGORY
        ORDER BY TICKET_COUNT DESC
    """
    return run_query(query)


def get_tickets_by_priority(category: str, priority: str, start_date, end_date) -> pd.DataFrame:
    """Get ticket counts by priority."""
    conditions = [f"CREATED_DATE BETWEEN '{start_date}' AND '{end_date}'"]

    if category != "All":
        conditions.append(f"CATEGORY = '{category}'")
    if priority != "All":
        conditions.append(f"PRIORITY = '{priority}'")

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT PRIORITY, COUNT(*) as TICKET_COUNT
        FROM {TABLE_NAME}
        WHERE {where_clause}
        GROUP BY PRIORITY
        ORDER BY TICKET_COUNT DESC
    """
    return run_query(query)


# Main app
def main():
    st.title("🎫 Support Ticket Dashboard")
    st.markdown("Analyze and search support tickets from your Snowflake database")

    # Sidebar filters
    st.sidebar.header("Filters")

    # Load filter options
    try:
        categories = get_categories()
        priorities = get_priorities()
        min_date, max_date = get_date_range()
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        st.info("Please ensure the app has access to the required database and tables.")
        st.stop()

    # Filter widgets
    selected_category = st.sidebar.selectbox("Category", categories)
    selected_priority = st.sidebar.selectbox("Priority", priorities)

    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # Handle single date selection
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range

    # Search section
    st.markdown("---")
    search_query = st.text_input(
        "🔍 Search tickets",
        placeholder="Search by description, issue type, or keywords..."
    )

    # If searching, show search results
    if search_query:
        st.subheader(f"Search Results for: '{search_query}'")
        with st.spinner("Searching..."):
            search_results = search_tickets_cortex(search_query)

        if len(search_results) > 0:
            st.success(f"Found {len(search_results)} matching tickets")

            for _, row in search_results.iterrows():
                with st.expander(f"🎫 {row['TICKET_ID']} - {row['CATEGORY']} ({row['PRIORITY']})"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Customer ID:** {row['CUSTOMER_ID']}")
                        st.write(f"**Category:** {row['CATEGORY']}")
                        st.write(f"**Subcategory:** {row.get('SUBCATEGORY', 'N/A')}")
                    with col2:
                        st.write(f"**Priority:** {row['PRIORITY']}")
                        st.write(f"**Created:** {row['CREATED_DATE']}")
                    if 'DESCRIPTION' in row and row['DESCRIPTION']:
                        st.write(f"**Description:** {row['DESCRIPTION']}")
        else:
            st.warning("No tickets found matching your search.")

    else:
        # Show dashboard when not searching
        st.markdown("---")

        # Metrics row
        col1, col2, col3, col4 = st.columns(4)

        with st.spinner("Loading metrics..."):
            total_query = f"""
                SELECT COUNT(*) as total FROM {TABLE_NAME}
                WHERE CREATED_DATE BETWEEN '{start_date}' AND '{end_date}'
            """
            if selected_category != "All":
                total_query = total_query.replace("WHERE", f"WHERE CATEGORY = '{selected_category}' AND")
            if selected_priority != "All":
                total_query = total_query.replace("WHERE", f"WHERE PRIORITY = '{selected_priority}' AND")

            total_df = run_query(total_query)
            total_tickets = total_df["TOTAL"].iloc[0]

        with col1:
            st.metric("Total Tickets", f"{total_tickets:,}")
        with col2:
            st.metric("Categories", len(categories) - 1)
        with col3:
            st.metric("Date Range", f"{(end_date - start_date).days} days")
        with col4:
            avg_per_day = total_tickets / max((end_date - start_date).days, 1)
            st.metric("Avg/Day", f"{avg_per_day:.0f}")

        st.markdown("---")

        # Charts section
        st.subheader("📊 Ticket Trends")

        # Time series chart
        with st.spinner("Loading charts..."):
            time_df = get_tickets_over_time(selected_category, selected_priority, start_date, end_date)

        if len(time_df) > 0:
            fig_time = px.line(
                time_df,
                x="CREATED_DATE",
                y="TICKET_COUNT",
                title="Tickets Over Time",
                labels={"CREATED_DATE": "Date", "TICKET_COUNT": "Ticket Count"}
            )
            fig_time.update_layout(hovermode="x unified")
            st.plotly_chart(fig_time, use_container_width=True)

        # Category and Priority charts side by side
        col1, col2 = st.columns(2)

        with col1:
            cat_df = get_tickets_by_category(selected_category, selected_priority, start_date, end_date)
            if len(cat_df) > 0:
                fig_cat = px.bar(
                    cat_df,
                    x="TICKET_COUNT",
                    y="CATEGORY",
                    orientation="h",
                    title="Tickets by Category",
                    labels={"TICKET_COUNT": "Count", "CATEGORY": "Category"}
                )
                fig_cat.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_cat, use_container_width=True)

        with col2:
            pri_df = get_tickets_by_priority(selected_category, selected_priority, start_date, end_date)
            if len(pri_df) > 0:
                fig_pri = px.pie(
                    pri_df,
                    values="TICKET_COUNT",
                    names="PRIORITY",
                    title="Tickets by Priority"
                )
                st.plotly_chart(fig_pri, use_container_width=True)

        st.markdown("---")

        # Ticket list section
        st.subheader("📋 Recent Tickets")

        with st.spinner("Loading tickets..."):
            tickets_df = get_filtered_tickets(
                selected_category,
                selected_priority,
                start_date,
                end_date
            )

        if len(tickets_df) > 0:
            # Display as expandable cards
            for _, row in tickets_df.iterrows():
                with st.expander(
                    f"🎫 {row['TICKET_ID']} | {row['CATEGORY']} | {row['PRIORITY']} | {row['CREATED_DATE']}"
                ):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.write(f"**Ticket ID:** {row['TICKET_ID']}")
                        st.write(f"**Customer ID:** {row['CUSTOMER_ID']}")
                        st.write(f"**Account ID:** {row.get('ACCOUNT_ID', 'N/A')}")
                    with col2:
                        st.write(f"**Category:** {row['CATEGORY']}")
                        st.write(f"**Subcategory:** {row.get('SUBCATEGORY', 'N/A')}")
                        st.write(f"**Priority:** {row['PRIORITY']}")
                    with col3:
                        st.write(f"**Created Date:** {row['CREATED_DATE']}")
                        st.write(f"**Region:** {row.get('GEO_ID', 'N/A')}")
        else:
            st.info("No tickets found with the selected filters.")


if __name__ == "__main__":
    main()
