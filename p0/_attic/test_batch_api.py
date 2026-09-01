import requests
import json

url = "http://localhost:8000/generate_batch"
payload = {
    "items": [
        {
            "name": "Limestone Crusher – Impact Type",
            "description": "Heavy-duty steel housing with a high-speed rotating rotor and impact hammers breaking large limestone chunks.",
        },
        {
            "name": "Crusher Main Motor 1500kW",
            "description": "Large industrial electric motor with cooling fins and coupling driving the crusher rotor.",
        },
        {
            "name": "Crusher Gearbox",
            "description": "Compact enclosed gear unit with shafts and lubrication lines transmitting torque from motor to crusher.",
        },
        {
            "name": "Crusher Hydraulic Unit",
            "description": "Skid-mounted oil reservoir with pumps, valves, and cylinders for gap adjustment and safety release.",
        },
        {
            "name": "Vertical Roller Mill – Raw Material",
            "description": "Tall cylindrical mill with grinding table and rollers applying pressure on material bed.",
        },
        {
            "name": "Raw Mill Main Motor 4500kW",
            "description": "Massive high-power motor connected to mill gearbox via coupling.",
        },
        {
            "name": "Raw Mill Gearbox",
            "description": "Large vertical gearbox with planetary stages supporting grinding table rotation.",
        },
        {
            "name": "Raw Mill Fan",
            "description": "Large centrifugal fan with spiral casing pulling process air through the mill.",
        },
        {
            "name": "Raw Mill Separator",
            "description": "Cylindrical classifier with rotating cage separating fine and coarse particles.",
        },
        {
            "name": "Raw Mill Hydraulic System",
            "description": "Hydraulic station with accumulators and cylinders controlling roller pressure.",
        },
        {
            "name": "5-Stage Preheater with Precalciner",
            "description": "Tower structure with stacked cyclone separators and calciner vessel for heat exchange.",
        },
        {
            "name": "Preheater ID Fan 3000kW",
            "description": "High-capacity induced draft fan extracting gases through the preheater tower.",
        },
        {
            "name": "Calciner Tertiary Air Fan",
            "description": "Medium-sized fan supplying hot air from cooler to calciner.",
        },
        {
            "name": "Downcomer Damper Actuator",
            "description": "Motorized actuator mounted on duct damper controlling gas flow direction.",
        },
        {
            "name": "Rotary Kiln 4.8m x 72m",
            "description": "Long rotating steel cylinder with refractory lining inclined slightly for clinker formation.",
        },
        {
            "name": "Kiln Main Drive Motor 800kW",
            "description": "Heavy-duty motor driving kiln rotation via girth gear.",
        },
        {
            "name": "Kiln Main Gearbox",
            "description": "Robust gearbox transmitting torque to kiln girth gear.",
        },
        {
            "name": "Kiln Auxiliary Drive Motor 50kW",
            "description": "Small backup motor for slow kiln rotation during maintenance.",
        },
        {
            "name": "Kiln Burner",
            "description": "Nozzle assembly injecting fuel and air into kiln for combustion.",
        },
        {
            "name": "Kiln Tyre #1 (Feed End)",
            "description": "Large steel ring supporting kiln shell at inlet side.",
        },
        {
            "name": "Kiln Tyre #2 (Mid)",
            "description": "Central support ring maintaining kiln alignment.",
        },
        {
            "name": "Kiln Tyre #3 (Discharge End)",
            "description": "Support ring near clinker outlet end.",
        },
        {
            "name": "Kiln Thrust Roller Assembly",
            "description": "Inclined rollers restricting axial movement of kiln shell.",
        },
        {
            "name": "Grate Clinker Cooler",
            "description": "Horizontal grate bed with moving plates cooling hot clinker using air flow.",
        },
        {
            "name": "Cooler Vent Fan 1",
            "description": "Centrifugal fan extracting hot gases from cooler.",
        },
        {
            "name": "Cooler Vent Fan 2",
            "description": "Secondary exhaust fan maintaining airflow balance.",
        },
        {
            "name": "Cooler Hammer Crusher",
            "description": "Rotor with hammers crushing clinker lumps at cooler discharge.",
        },
        {
            "name": "Cooler Drag Chain Conveyor",
            "description": "Enclosed conveyor with chains dragging clinker along trough.",
        },
        {
            "name": "Cement Ball Mill #1 (Open Circuit)",
            "description": "Rotating cylindrical drum filled with steel balls for grinding clinker.",
        },
        {
            "name": "Cement Mill Main Motor 5000kW",
            "description": "High-power motor driving mill via gear system.",
        },
        {
            "name": "Cement Mill Gearbox",
            "description": "Gear reducer transmitting motion to mill shell.",
        },
        {
            "name": "Cement Mill Separator (SEPAX)",
            "description": "Dynamic separator with rotating cage for particle classification.",
        },
        {
            "name": "Cement Mill Vent Fan",
            "description": "Fan drawing air through mill for drying and transport.",
        },
        {
            "name": "Cement Mill SEPAX Fan",
            "description": "Dedicated fan supplying air to separator system.",
        },
        {
            "name": "Discharge Elevator",
            "description": "Vertical bucket conveyor lifting material from mill outlet.",
        },
        {
            "name": "Final Product Elevator",
            "description": "Bucket elevator transporting finished cement to silos.",
        },
        {
            "name": "Cement Ball Mill #2 (Closed Circuit)",
            "description": "Ball mill integrated with separator for recirculating coarse material.",
        },
        {
            "name": "Cement Mill #2 Main Motor 4200kW",
            "description": "Electric motor driving second mill system.",
        },
        {
            "name": "Cement Mill #2 Gearbox",
            "description": "Gear unit transmitting power to mill.",
        },
        {
            "name": "Cement Mill #2 Separator",
            "description": "Classifier separating fine cement from coarse particles.",
        },
        {
            "name": "Cement Mill #2 Fan",
            "description": "Process fan maintaining airflow in closed circuit grinding.",
        },
        {
            "name": "Coal Vertical Roller Mill",
            "description": "Compact vertical mill grinding coal with rollers and hot air drying.",
        },
        {
            "name": "Coal Mill Motor 1200kW",
            "description": "Medium-capacity motor driving coal mill.",
        },
        {
            "name": "Coal Mill Gearbox",
            "description": "Vertical gearbox transmitting torque to grinding table.",
        },
        {
            "name": "Coal Mill Fan",
            "description": "Fan supplying hot air for coal drying and transport.",
        },
        {
            "name": "Rotary Packer 8-Spout",
            "description": "Circular rotating machine with multiple spouts filling cement bags.",
        },
        {
            "name": "Packer Drive Motor",
            "description": "Motor rotating the packer carousel.",
        },
        {
            "name": "Bag Counter",
            "description": "Electronic unit counting filled cement bags on conveyor.",
        },
    ]
}


try:
    response = requests.post(url, json=payload, timeout=300)
    print(f"Status Code: {response.status_code}")
    print(json.dumps(response.json(), indent=2))
except Exception as e:
    print(f"Error: {e}")
