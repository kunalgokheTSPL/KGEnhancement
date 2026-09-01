import os
import re
import google.generativeai as genai
from dotenv import load_dotenv

env_path = ".env.local"
if not os.path.exists(env_path):
    env_path = os.path.join("..", ".env.local")

load_dotenv(dotenv_path=env_path)

api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY not found in .env.local")

genai.configure(api_key=api_key)

SYSTEM_PROMPT = """
You are a coding agent working in a refinery plant. You understand DCS system visuals.
Follow the below code below.

Below is a reference component for an Intercooler:

```tsx
import React from 'react';

interface IntercoolerProps {
    x?: number;
    y?: number;
    scale?: number;
    status?: 'normal' | 'warning' | 'hazard';
    label?: string;
    onPointerDown?: (e: React.PointerEvent) => void;
}

const Intercooler = ({
    x = 0,
    y = 0,
    scale = 1,
    status = 'normal',
    label = "v-2430A",
    onPointerDown,
}: IntercoolerProps) => {
    const isHazard = status === 'hazard';
    const statusColor = isHazard ? '#ef4444' : status === 'warning' ? '#eab308' : '#0369a1';

    return (
        <g transform={`translate(${x}, ${y}) scale(${scale})`} onPointerDown={onPointerDown} className="cursor-move">
            <rect width="120" height="45" rx="4" fill="#f8fafc" stroke="#1e293b" strokeWidth="2.5"/>
            <path d="M 10,22.5 L 25,10 L 40,35 L 55,10 L 70,35 L 85,10 L 100,35 L 110,22.5"
                fill="none" stroke="#94a3b8" strokeWidth="2" strokeLinejoin="round"/>
            <rect x="25" y="-8" width="12" height="8" fill="#334155" />
            <rect x="85" y="45" width="12" height="8" fill="#334155" />
            <rect x="-8" y="15" width="8" height="15" fill="#334155" />
            <rect x="120" y="15" width="8" height="15" fill="#334155" />
            <text x="42" y="-15" textAnchor="middle"
                className="fill-slate-600 text-[18px] font-mono font-black uppercase tracking-tighter pointer-events-none select-none">
                {label}
            </text>
            <circle cx="135" cy="0" r="4" fill={statusColor}>
                <animate attributeName="opacity" values="1;0.4;1"
                    dur={isHazard ? "0.6s" : "2s"} repeatCount="indefinite"/>
            </circle>
        </g>
    );
};

export default Intercooler;

Your Job is to generate Highly detailed Equipments which look exactly in DCS screen.

CRITICAL RULES FOR FRONTEND PARSER (MUST FOLLOW):
1. NO Array Mapping: Do NOT use `.map()` or any array loops inside your JSX. If you need 3 items (like data readouts), write out all 3 `<g>` elements manually line by line!
2. NO Javascript Expressions: Do NOT use ternary operators evaluating state like `stroke={isNormal ? 'red' : 'blue'}` inside the JSX.
3. STRICT Dynamic Variables: The ONLY variables you are allowed to insert inside JSX properties are exactly `{statusColor}` and `{label}` (e.g. `stroke={statusColor}`). All other styling must be static/hardcoded strings.
4. If you want animations or alerts, rely on `{statusColor}` applied to basic `stroke` or `fill`.

"""


def generate_component(equipment_name: str, equipment_desc: str):
    prompt = f"{SYSTEM_PROMPT}\n\nGENERATE NEW EQUIPMENT: {equipment_name} \n\n Equipment Description: {equipment_desc}"
    model = genai.GenerativeModel(os.getenv("GEMINI_MODEL", "gemini-3.6-flash"))
    response = model.generate_content(prompt)
    text = response.text

    clean_code = re.sub(r"```(tsx|jsx|javascript|typescript)?", "", text)
    clean_code = re.sub(r"```", "", clean_code).strip()

    return clean_code
