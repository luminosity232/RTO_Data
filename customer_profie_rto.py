import streamlit as st
import pandas as pd
from databricks import sql
from jinja2 import Template

# -------------------------------
# 1. Define Database Connection Info
# -------------------------------
DB_HOSTNAME = "dbc-f63f6952-da5d.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/012daaf7ff1e0303"
ACCESS_TOKEN = "dapi803d3807f88a3fb59e5d948b7e3f6036"

# -------------------------------
# Optional: Define pincode info mapping
# -------------------------------
pincode_info = {
    "122001": {"city": "Gurgaon", "state": "Haryana", "district": "Gurgaon"},
    "201301": {"city": "Noida", "state": "Uttar Pradesh", "district": "Ghaziabad"},
    "421302": {"city": "Pune", "state": "Maharashtra", "district": "Pune"},
    "122506": {"city": "Gurgaon", "state": "Haryana", "district": "Gurgaon"},
    "560064": {"city": "Bangalore", "state": "Karnataka", "district": "Bangalore Urban"},
    "122004": {"city": "Gurgaon", "state": "Haryana", "district": "Gurgaon"},
    "248001": {"city": "Muzaffarnagar", "state": "Uttar Pradesh", "district": "Muzaffarnagar"},
    "560068": {"city": "Bangalore", "state": "Karnataka", "district": "Bangalore Urban"}
}

# -------------------------------
# 2. Define Your Query Template and Parameters
# -------------------------------
sql_template = """
SELECT
    SUM(CASE WHEN t.rto_mark_date IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS rto_rate
FROM
    clickpost_poc_may_18_v2.bronze.shipment_static_attribute o
JOIN
    clickpost_poc_may_18_v2.bronze.shipment_attribute t ON o.tracking_info_id = t.tracking_info_id
LEFT JOIN 
    clickpost_poc_may_18_v2.silver.ml_feature__pincode_master pm_drop
    ON TRY_CAST(o.drop_pin_code AS BIGINT) = pm_drop.pincode
WHERE
    1=1
    {% if drop_pin %}
      AND o.drop_pin_code = '{{ drop_pin }}'
    {% endif %}
    {% if invoice_value_min %}
      AND o.invoice_value >= {{ invoice_value_min }}
    {% endif %}
    {% if order_type %}
      AND o.order_type = {{ order_type }}
    {% endif %}
    {% if enterprise_id %}
      AND o.enterprise_user_profile_id = {{ enterprise_id }}
    {% endif %}
    {% if carrier_partner %}
      AND o.three_pl_cp_name = '{{ carrier_partner }}'
    {% endif %}
"""

# -------------------------------
# 3. Streamlit UI for Input Parameters
# -------------------------------
st.title("RTO Data")

# Dynamic lookup for Drop Pincode using the provided list
drop_pincode_list = ["", "122001", "201301", "421302", "122506", "560064", "122004", "248001", "560068"]
drop_pin = st.selectbox("Drop Pincode", options=drop_pincode_list)

# If a valid drop pincode is selected, show its city, state, and district in the sidebar.
if drop_pin and drop_pin in pincode_info:
    info = pincode_info[drop_pin]
    st.sidebar.markdown("### Pincode Information")
    st.sidebar.write("City:", info["city"])
    st.sidebar.write("State:", info["state"])
    st.sidebar.write("District:", info["district"])

# Invoice Value Minimum - text input (converted to int if provided)
invoice_value_min_str = st.text_input("Invoice Value Minimum", "")

# Order Type - dynamic lookup using names; UI shows "Prepaid" and "COD" but SQL uses numeric values.
order_type_map = {"Prepaid": 1, "COD": 2}
order_type_names = [""] + list(order_type_map.keys())
selected_order_type = st.selectbox("Order Type", options=order_type_names)
order_type = order_type_map[selected_order_type] if selected_order_type != "" else None

# Dynamic lookup for Enterprise Name
enterprise_map = {
    "meesho-ndr": 289,
    "Nykaa": 13,
    "meesho-npr": 180573,
    "Supply": 115,
    "purplle": 32,
    "Nykaa-Fashion": 109,
    "nushop": 30445,
    "1mg": 74,
    "Herbalife": 71,
    "truemeds": 225
}
enterprise_names = [""] + list(enterprise_map.keys())
selected_enterprise = st.selectbox("Enterprise", options=enterprise_names)
enterprise_id = enterprise_map[selected_enterprise] if selected_enterprise != "" else None

# Carrier Partner - dynamic lookup using a selectbox from a full list
carrier_partner_list = [
    "XpressBees",
    "Proship B2C",
    "DHL [INT]",
    "Emirates Post",
    "Trackon B2B",
    "Transeazy Logistics MPS",
    "Fedex MPS [Crossborder]",
    "Ninjavan",
    "Fynd HLD",
    "Xindus",
    "Shipsy",
    "Bluedart B2B MPS",
    "ST Courier",
    "Blowhorn HLD",
    "Holisol",
    "Shiprocket Reverse",
    "SMSA Express",
    "Shadowfax Now Slot HLD",
    "J&T (Jet Express)",
    "Trust",
    "Fedex[INT] MPS",
    "Delhivery",
    "Shadowfax Reverse",
    "XpressBees Cargo MPS [B2B]",
    "Loginext HLD",
    "TVS Supply chain B2B",
    "Fareye",
    "Loadshare",
    "Quiqup [ME]",
    "Fedex India [Domestic and Crossborder]",
    "Essential MPS",
    "Bluedart International",
    "First Flight MPS",
    "FShip",
    "theNet MPS [ME]",
    "Locus B2B Reverse",
    "Nimbus Post",
    "SVG",
    "Whizzard B2C SPS",
    "Saudi Post MPS",
    "Hackle Hub Reverse",
    "Fretex MPS",
    "Vamaship Reverse",
    "Bluedart",
    "Emiza-Shippigo",
    "XpressBees Prepaid Wallet",
    "Stackbox",
    "Online Express",
    "TimeExpress",
    "Porter HLD",
    "Criticalog MPS",
    "Shiprocket Cargo B2B",
    "SME Cargo",
    "Natayu Express",
    "DHL",
    "VRL Logisitics B2B MPS",
    "Sicepat",
    "Blitz Reverse HLD",
    "Om Logistics MPS",
    "Delhivery Reverse",
    "Delhivery HLD",
    "Movin B2B MPS",
    "Purpledrone B2C Reverse",
    "Trackon",
    "Ekart Logistics Large SPS",
    "Self [MPS]",
    "Smartship",
    "Shree Maruti Courier",
    "TForce MPS",
    "Bluedart MPS Reverse [B2B]",
    "Shipsy MPS Reverse",
    "DTDC Reverse",
    "Holisol Reverse",
    "DHL MPS Reverse [INT]",
    "DHL Reverse [INT]",
    "Flow Express B2B MPS [INT]",
    "Rapidshyp HLD",
    "Shri karni B2C SPS",
    "Ajex Logistics INT MPS [SA]",
    "EcomExpress",
    "SB Express Cargo B2B",
    "Kargokart",
    "PicoXpress",
    "Pepcart",
    "Swift Reverse",
    "First Logistics",
    "Gati B2B MPS",
    "Ekart Logistics Large MPS",
    "TCI MPS",
    "Jeebly Reverse",
    "Sequel",
    "Abhilaya (One World Logistics) [B2C]",
    "Rivigo B2B",
    "Fareye Ekart B2B",
    "Lion Parcel",
    "KGP Express",
    "EcomExpress Reverse",
    "DHL MPS [INT]",
    "Shipdelight",
    "Delhivery B2C MPS",
    "TCI",
    "Wefast/Borzo HLD",
    "Jetline",
    "A L Services",
    "Naqel Express [ME]",
    "Shipa Delivery Reverse",
    "CMS",
    "GMTC [ME]",
    "UPS [INT & Domestic]",
    "SkyExpress International MPS",
    "Aramex MPS INT Reverse",
    "Go Go Express",
    "Sahara Packers",
    "Franch Express",
    "Starlinks MPS",
    "Time Express Reverse",
    "Jeebly",
    "DP World SPDS",
    "Shiprocket",
    "Ekart Logistics",
    "Pidge HLD",
    "XP India",
    "Delex MPS",
    "Elite Eagle B2B",
    "BVC eShip",
    "Zippee B2C SPS",
    "Gms Logistic",
    "Porter Express",
    "Fodel MPS [ME]",
    "The Professional Couriers",
    "Aramex INT Reverse",
    "Shipdelight Reverse",
    "Safexpress B2B MPS",
    "Universal Logistics",
    "BVC eShip Reverse",
    "BVC ValSHIP B2B SPS",
    "DHL India Cross Border SPS",
    "Elite Express Reverse [ME]",
    "Ekart Reverse",
    "Shipway Forward  B2C SPS",
    "XpressBees Reverse",
    "Pikndel HLD",
    "Naplog B2B Reverse",
    "Criticalog",
    "Shipsy MPS",
    "Shadowfax Marketplace HLD",
    "Fedex [INT]",
    "Smsa Express MPS",
    "Jeebly MPS",
    "Avikam Logistics B2B",
    "DTDC MPS",
    "Shipyaari",
    "Shadowfax",
    "ATS (Amazon Transportation Services)",
    "DP WORLD (Delcart) Reverse",
    "SkyExpress MPS [ME]",
    "Aramex INT MPS",
    "Nimbus Post Reverse",
    "Delhivery B2C MPS Reverse",
    "SMSA Express Reverse",
    "McCollister",
    "Fenix Express SPS [ME]",
    "Bluedart Reverse",
    "WowExpress",
    "Arrow Express MPS",
    "ElasticRun",
    "Relay Express MPS",
    "J&T (Jet Express) [ME]",
    "Kale Goods B2B",
    "Ithink Logistics",
    "Dependo",
    "Delhivery B2B MPS",
    "Quickshift",
    "Shadowfax B2B",
    "Dependo MPS",
    "IndiaPost - Deprecated",
    "Elite [ME]",
    "SkyExpress Reverse [ME]",
    "Dox & Pak",
    "Indopaket",
    "DHL India Cross Border MPS",
    "Madhur Courier",
    "RedBox MPS",
    "Salasa MPS",
    "Bizlog Reverse",
    "DTDC",
    "Shree Maruti SMILE Ecomm",
    "Blitz (Formerly Grow Simplee)",
    "Scorpion Express B2B MPS",
    "IndiaPost",
    "Aymakan",
    "Aramex INT",
    "Naplog B2B",
    "Proship B2C Reverse",
    "Purpledrone",
    "JNE Express",
    "Big Basket HLD",
    "RPX Express",
    "Starlinks",
    "Self Reverse",
    "SELF",
    "Swift",
    "Shipsy Reverse",
    "Eshopbox",
    "Vamaship",
    "Aum Logistic",
    "DTDC [LTL] MPS",
    "Mass Express Cargo B2B",
    "Fareye Reverse",
    "SAP Express",
    "Ithink Logistics Reverse",
    "Starlinks Reverse",
    "XpressBees Prepaid Wallet Reverse",
    "SAAP Logistics B2B MPS",
    "Vxpress B2B",
    "Viahero",
    "Consegnia",
    "Qwqer HLD",
    "Qwqer",
    "Roadcast B2C MPS",
    "QMS Courier",
    "Fedex[INT] Reverse",
    "RAW express INT",
    "Lexship International",
    "Safexpress B2B SPS",
    "DTDC MPS Reverse",
    "A L Services Reverse",
    "Online Express Reverse",
    "Fenix Express Reverse SPS [ME]",
    "UPS Reverse [INT & Domestic]",
    "Shipease",
    "Sequel Reverse",
    "Porter Express Reverse",
    "PCP express",
    "Smartr Logistics",
    "Naqel Express MPS [INT]",
    "Skip Express MPS",
    "R World Logistics",
    "Time Express B2B",
    "Fenix MPS",
    "Starlinks MPS Reverse",
    "FedEx TNT MPS",
    "RedBox Reverse",
    "Shyplite",
    "Big Guy Logistics",
    "Pickrr",
    "Quiqup Reverse [ME]",
    "iMile",
    "SM Express B2B MPS",
    "Mudita B2B",
    "Smartr Logistics Reverse",
    "Delhivery International",
    "KerryIndev MPS",
    "Sampark B2B",
    "WareIQ",
    "Saee",
    "Skip MPS Reverse",
    "Ninjavan Reverse",
    "Fenix MPS Reverse",
    "Shipease Reverse",
    "Thabit Logistics",
    "Postage",
    "Eshopbox Reverse",
    "CMS Reverse",
    "Linker",
    "Bizlog SPS",
    "Delhivery B2B MPS Reverse",
    "The ARC B2B MPS",
    "Criticalog SPS Reverse",
    "Delivery Plus Reverse",
    "XpressBees Ecom MPS [B2C]",
    "Fynd B2B",
    "Porter Express MPS",
    "Madhur Parcels B2B",
    "Roadcast Reverse",
    "Shipa Delivery",
    "Amazon - Direct Fulfilment MPS",
    "Smartr Wheeler B2B MPS",
    "SpeedoPost",
    "Shipyaari B2B",
    "Adloggs HLD",
    "Piekart B2C SPS",
    "Lalji Mulji B2B",
    "ABT Parcel",
    "Postaplus Reverse",
    "Shiphero/ShipOx",
    "The Professional Courier B2B",
    "Postaplus B2B",
    "Consegnia Reverse",
    "Noka Freight B2B",
    "DTDC INT MPS",
    "Hackle Hub",
    "Valmo",
    "KerryIndev",
    "Doordash MPS HLD",
    "MPCL Courier B2B",
    "Shiprocket HLD",
    "Salasa MPS Reverse",
    "Shiplog HLD",
    "Oxyzen B2B MPS",
    "Kangaroo MPS",
    "Swiggy HLD",
    "Atlantic International",
    "Kuehne Nagel",
    "Blowhorn HLD Reverse",
    "First Flight MPS Reverse",
    "SpotOn",
    "DBS Courier",
    "Sitics B2B",
    "Bluedart MPS Reverse [B2C]",
    "Fasttrack B2B",
    "Shipdocket",
    "Ecom express B2B",
    "Shiperfecto",
    "Fynd Reverse",
    "TCI Freight B2B",
    "M Express B2B (Neotech)",
    "Shipdocket Reverse",
    "Frotierrag B2B",
    "Sameday",
    "DTDC INT",
    "Sameday MPS",
    "Shipmozo HLD",
    "Saudi Post",
    "Parekh Courier B2B",
    "USPS SCRAPPER",
    "Ninjavan B2B",
    "Shyam Courier",
    "Radiant Logistics",
    "Roadcross Logistics",
    "Express parcel delivery (EPS) B2C SPS",
    "Zomato HLD",
    "Local Wheels B2B",
    "Urbane Bolt",
    "Shree Raj Courier B2B",
    "Mover HLD",
    "Atlantic",
    "Baba Express",
    "V-Trans Logistics B2B",
    "SkyExpress Reverse MPS [ME]",
    "TForce",
    "Motoray Mobility",
    "Self FTL",
    "SMILE Ecomm (Shree maruti) Reverse",
    "UPS MPS [INT & Domestic]",
    "City link",
    "Zypp Electric HLD",
    "ElasticRun Reverse",
    "SkyExpress International Reverse MPS",
    "Pickupp INT",
    "Velocity Express B2B",
    "Proship B2B MPS",
    "SME Cargo Reverse",
    "Fedex International FIC",
    "JBC Logistics INT",
    "Emiza-Shippigo Reverse",
    "Fedex[INT] MPS Reverse",
    "GLS International",
    "DPD Netherlands",
    "RedBox MPS Reverse",
    "PicoXpress MPS",
    "AGL [ME]",
    "Abhilaya (One World Logistics) Reverse [B2C]",
    "Intelcom B2B SPS",
    "Doordash HLD",
    "Shadowfax Heavy B2B",
    "Amaze Solutions",
    "WholeMark",
    "GMTC Reverse",
    "Evri",
    "Evri INT",
    "Evri Reverse",
    "Telyport HLD",
    "NuvoEx Reverse",
    "InnovEx",
    "Uber HLD",
    "Uber MPS HLD",
    "Ekart Logistics B2B",
    "Airmount Logistics B2B"
]
carrier_partner = st.selectbox("Carrier Partner", options=[""] + carrier_partner_list)

# -------------------------------
# 4. Convert Numeric Inputs (if provided)
# -------------------------------
try:
    invoice_value_min = int(invoice_value_min_str) if invoice_value_min_str.strip() != "" else None
except ValueError:
    st.error("Invoice Value Minimum must be an integer.")
    invoice_value_min = None

def none_if_empty(val):
    return val if isinstance(val, str) and val.strip() != "" else None

# -------------------------------
# 5. Build Input Parameters and Render Query
# -------------------------------
input_params = {
    "drop_pin": none_if_empty(drop_pin),
    "invoice_value_min": invoice_value_min,
    "order_type": none_if_empty(str(order_type)) if order_type is not None else None,
    "enterprise_id": enterprise_id,
    "carrier_partner": none_if_empty(carrier_partner),
}

template = Template(sql_template)
query = template.render(**input_params)

# -------------------------------
# 6. Execute the Query and Display the RTO Rate
# -------------------------------
if st.button("Run Query"):
    try:
        with sql.connect(
            server_hostname=DB_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                column_names = [col[0] for col in cursor.description]
                df_result = pd.DataFrame(result, columns=column_names)
        
        if not df_result.empty:
            rto_rate = df_result.iloc[0]['rto_rate']
            st.subheader("RTO Rate")
            if rto_rate is not None:
                st.write(f"{rto_rate:.2f}%")
            else:
                st.write("RTO Rate not computed (returned value is None).")
        else:
            st.write("No data returned.")
    except Exception as e:
        st.error(f"Error executing query: {e}")
