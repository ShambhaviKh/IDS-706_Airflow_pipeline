from datetime import datetime, timedelta
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Postgres and Redis hooks
try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Paths
def get_base_dir():
    if os.path.exists("/opt/airflow/dags"):
        return "/opt/airflow"
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()
DATA_DIR = os.path.join(BASE_DIR, "data")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
ANALYSIS_DIR = os.path.join(BASE_DIR, "analysis")

for dir_path in [PROCESSED_DIR, ANALYSIS_DIR]:
    os.makedirs(dir_path, exist_ok=True)

# === Functions ===
def process_orders():
    filepath = os.path.join(DATA_DIR, "orders.csv")
    if not os.path.exists(filepath):
        print(f"{filepath} not found")
        return
    df = pd.read_csv(filepath)
    df["Date Order was placed"] = pd.to_datetime(df["Date Order was placed"], errors='coerce')
    df["Delivery Date"] = pd.to_datetime(df["Delivery Date"], errors='coerce')
    df["profit"] = df["Total Retail Price for This Order"] - (df["Cost Price Per Unit"] * df["Quantity Ordered"])
    df["order_month"] = df["Date Order was placed"].dt.to_period("M").astype(str)
    df.to_csv(os.path.join(PROCESSED_DIR, "orders_clean.csv"), index=False)

def process_products():
    filepath = os.path.join(DATA_DIR, "product-supplier.csv")
    if not os.path.exists(filepath):
        print(f"{filepath} not found")
        return
    df = pd.read_csv(filepath)
    df.to_csv(os.path.join(PROCESSED_DIR, "products_clean.csv"), index=False)

def merge_datasets():
    orders_path = os.path.join(PROCESSED_DIR, "orders_clean.csv")
    products_path = os.path.join(PROCESSED_DIR, "products_clean.csv")
    if not os.path.exists(orders_path) or not os.path.exists(products_path):
        print("Processed files missing, skipping merge")
        return
    orders = pd.read_csv(orders_path)
    products = pd.read_csv(products_path)
    merged = orders.merge(products, on="Product ID", how="left")
    summary = merged.groupby(["Customer ID", "Product Group"], as_index=False).agg(
        total_spent=("Total Retail Price for This Order", "sum"),
        total_profit=("profit", "sum"),
        num_orders=("Order ID", "count"),
        avg_order_value=("Total Retail Price for This Order", "mean")
    )
    summary.to_csv(os.path.join(PROCESSED_DIR, "customer_product_stats.csv"), index=False)

def load_to_postgres():
    if not POSTGRES_AVAILABLE:
        return
    hook = PostgresHook(postgres_conn_id="local_postgres")
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_csv(os.path.join(PROCESSED_DIR, "customer_product_stats.csv"))
    df.to_sql("fact_customer_products", engine, if_exists="replace", index=False)

def analyze_data():
    if not POSTGRES_AVAILABLE:
        return
    hook = PostgresHook(postgres_conn_id="local_postgres")
    sql = """
    SELECT "Product Group", AVG(total_spent) AS avg_spent, AVG(total_profit) AS avg_profit
    FROM fact_customer_products
    GROUP BY "Product Group"
    ORDER BY "Product Group";
    """
    df = hook.get_pandas_df(sql)
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(10, 6))
    df.plot(kind="bar", x="Product Group", y=["avg_spent", "avg_profit"], ax=ax)
    plt.tight_layout()
    plt.savefig(os.path.join(ANALYSIS_DIR, "spend_profit_by_group.png"))
    plt.close()

def push_to_redis():
    if not (REDIS_AVAILABLE and POSTGRES_AVAILABLE):
        return
    hook = PostgresHook(postgres_conn_id="local_postgres")
    sql = """
    SELECT "Product Group", COUNT(*) AS num_customers, AVG(total_spent) AS avg_spend
    FROM fact_customer_products
    GROUP BY "Product Group";
    """
    df = hook.get_pandas_df(sql)
    r = redis.Redis(host="redis", port=6379, db=0)
    for _, row in df.iterrows():
        key = f"product_group:{row['Product Group']}"
        value = {"num_customers": int(row["num_customers"]), "avg_spend": float(row["avg_spend"])}
        r.hset(key, mapping=value)
    r.set("last_update", datetime.utcnow().isoformat())

def cleanup_files():
    for f in ["orders_clean.csv", "products_clean.csv", "customer_product_stats.csv"]:
        path = os.path.join(PROCESSED_DIR, f)
        if os.path.exists(path):
            os.remove(path)

# === DAG Definition ===
default_args = {"owner": "shambhavi", "retries": 1, "retry_delay": timedelta(minutes=5), "depends_on_past": False}

with DAG(
    dag_id="retail_pipeline_dag",
    default_args=default_args,
    description="Airflow DAG for Orders & Products Analysis",
    schedule_interval="@daily",
    start_date=datetime(2024, 11, 1),
    catchup=False,
    tags=["assignment", "retail"]
) as dag:

    with TaskGroup("orders_processing") as orders_group:
        clean_orders = PythonOperator(task_id="process_orders", python_callable=process_orders)

    with TaskGroup("products_processing") as products_group:
        clean_products = PythonOperator(task_id="process_products", python_callable=process_products)

    merge = PythonOperator(task_id="merge_datasets", python_callable=merge_datasets)
    load = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    analyze = PythonOperator(task_id="analyze_data", python_callable=analyze_data)
    push_redis_task = PythonOperator(task_id="push_to_redis", python_callable=push_to_redis)
    cleanup = PythonOperator(task_id="cleanup_files", python_callable=cleanup_files)

    [orders_group, products_group] >> merge >> load >> analyze >> push_redis_task >> cleanup
