import ssl
import json
import logging
import urllib.request
import urllib.parse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("NiFiDeployer")

NIFI_BASE = "https://localhost:8443/nifi-api"

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def get_nifi_token(username="admin", password="Admin12345678!"):
    data = urllib.parse.urlencode({"username": username, "password": password}).encode("utf-8")
    req = urllib.request.Request(f"{NIFI_BASE}/access/token", data=data, method="POST")
    with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
        return resp.read().decode("utf-8")

def api_request(endpoint, token, method="GET", body=None):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(f"{NIFI_BASE}{endpoint}", data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))

def deploy_flow():
    token = get_nifi_token()
    logger.info("Authenticated with Apache NiFi REST API")

    # Get root process group
    root = api_request("/flow/process-groups/root", token)
    root_id = root["processGroupFlow"]["id"]
    logger.info(f"Targeting Root Process Group: {root_id}")

    # Check existing process groups
    groups = api_request(f"/process-groups/{root_id}/process-groups", token)
    for g in groups.get("processGroups", []):
        if g["component"]["name"] == "Crypto_Streaming_Pipeline":
            logger.info("Pipeline Process Group already exists in NiFi.")
            return g["id"]

    # 1. Create Process Group
    group_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Crypto_Streaming_Pipeline",
            "position": {"x": 300, "y": 100}
        }
    }
    new_group = api_request(f"/process-groups/{root_id}/process-groups", token, method="POST", body=group_payload)
    group_id = new_group["id"]
    logger.info(f"Created NiFi Process Group: 'Crypto_Streaming_Pipeline' (ID: {group_id})")

    # 2. Create InvokeHTTP (Producer: Fetch from Binance)
    fetch_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Fetch_Binance_Crypto_Ticks",
            "type": "org.apache.nifi.processors.standard.InvokeHTTP",
            "position": {"x": 300, "y": 100},
            "config": {
                "schedulingStrategy": "TIMER_DRIVEN",
                "schedulingPeriod": "10 sec",
                "properties": {
                    "HTTP Method": "GET",
                    "Remote URL": 'https://api.binance.com/api/v3/ticker/price?symbols=["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","ADAUSDT"]'
                },
                "autoTerminatedRelationships": ["No Retry", "Retry", "Original", "Failure"]
            }
        }
    }
    p_fetch = api_request(f"/process-groups/{group_id}/processors", token, method="POST", body=fetch_payload)
    fetch_id = p_fetch["id"]
    logger.info(f"Created Processor: Fetch_Binance_Crypto_Ticks (ID: {fetch_id})")

    # 3. Create SplitJson Processor
    split_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Split_Crypto_Ticks",
            "type": "org.apache.nifi.processors.standard.SplitJson",
            "position": {"x": 300, "y": 300},
            "config": {
                "properties": {
                    "JsonPath Expression": "$.*"
                },
                "autoTerminatedRelationships": ["failure", "original"]
            }
        }
    }
    p_split = api_request(f"/process-groups/{group_id}/processors", token, method="POST", body=split_payload)
    split_id = p_split["id"]
    logger.info(f"Created Processor: Split_Crypto_Ticks (ID: {split_id})")

    # 4. Create InvokeHTTP Processor (Consumer: Ingest to ClickHouse Bronze)
    post_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Ingest_To_ClickHouse_Bronze",
            "type": "org.apache.nifi.processors.standard.InvokeHTTP",
            "position": {"x": 300, "y": 500},
            "config": {
                "properties": {
                    "HTTP Method": "POST",
                    "Remote URL": "http://clickhouse:8123/?query=INSERT%20INTO%20bronze.bronze_crypto_stream(symbol,price)%20FORMAT%20JSONEachRow",
                    "Content-Type": "application/json",
                    "Basic Authentication Username": "admin",
                    "Basic Authentication Password": "admin123"
                },
                "autoTerminatedRelationships": ["No Retry", "Retry", "Original", "Failure", "Response"]
            }
        }
    }
    p_post = api_request(f"/process-groups/{group_id}/processors", token, method="POST", body=post_payload)
    post_id = p_post["id"]
    logger.info(f"Created Processor: Ingest_To_ClickHouse_Bronze (ID: {post_id})")

    # 5. Connect Fetch -> Split
    conn1_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Fetch_to_Split",
            "source": {"id": fetch_id, "type": "PROCESSOR", "groupId": group_id},
            "destination": {"id": split_id, "type": "PROCESSOR", "groupId": group_id},
            "selectedRelationships": ["Response"]
        }
    }
    api_request(f"/process-groups/{group_id}/connections", token, method="POST", body=conn1_payload)
    logger.info("Connected Fetch_Binance -> Split_Ticks")

    # 6. Connect Split -> Post
    conn2_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Split_to_ClickHouse",
            "source": {"id": split_id, "type": "PROCESSOR", "groupId": group_id},
            "destination": {"id": post_id, "type": "PROCESSOR", "groupId": group_id},
            "selectedRelationships": ["split"]
        }
    }
    api_request(f"/process-groups/{group_id}/connections", token, method="POST", body=conn2_payload)
    logger.info("Connected Split_Ticks -> ClickHouse_Ingest")

    # 7. Start Processors in the Group
    start_payload = {
        "id": group_id,
        "state": "RUNNING"
    }
    try:
        api_request(f"/flow/process-groups/{group_id}", token, method="PUT", body=start_payload)
        logger.info("Started NiFi Streaming Flow successfully!")
    except Exception as e:
        logger.warning(f"Note on starting group state: {e}")

    logger.info(f"NiFi flow ready and viewable at: https://localhost:8443/nifi/#/process-groups/{group_id}")
    return group_id

if __name__ == "__main__":
    deploy_flow()
