"""
Enhanced Theme System for Alpha Engine Dashboard
Provides sophisticated styling inspired by modern iOS design principles
"""

import streamlit as st
from typing import Dict, Any

# ========================
# SOPHISTICATED COLOR PALETTE
# ========================
COLORS = {
    # Neutral Palette (inspired by reference image)
    "neutral_50": "#FAFAFA",      # Very light grey - sophisticated backdrop
    "neutral_100": "#F5F5F5",     # Slightly darker for cards
    "neutral_200": "#EEEEEE",     # For hover states
    "neutral_300": "#E0E0E0",     # Subtle borders
    "neutral_400": "#BDBDBD",     # Standard borders
    "neutral_500": "#9E9E9E",     # Disabled elements
    "neutral_600": "#757575",     # Secondary text
    "neutral_700": "#616161",     # Primary text
    "neutral_800": "#424242",     # Emphasis text
    "neutral_900": "#212121",     # High contrast text
    
    # Primary Colors (sophisticated blue)
    "primary_50": "#E3F2FD",
    "primary_100": "#BBDEFB", 
    "primary_200": "#90CAF9",
    "primary_300": "#64B5F6",
    "primary_400": "#42A5F5",
    "primary_500": "#2196F3",     # Main primary
    "primary_600": "#1E88E5",
    "primary_700": "#1976D2",
    "primary_800": "#1565C0",     # Sophisticated primary
    "primary_900": "#0D47A1",
    
    # Semantic Colors
    "success_50": "#E8F5E8",
    "success_100": "#C8E6C9",
    "success_500": "#4CAF50",     # Mature green
    "success_700": "#2E7D32",     # Deep success
    
    "warning_50": "#FFF8E1",
    "warning_100": "#FFECB3",
    "warning_500": "#FF9800",     # Refined orange
    "warning_700": "#F57C00",     # Deep warning
    
    "error_50": "#FFEBEE",
    "error_100": "#FFCDD2", 
    "error_500": "#F44336",       # Standard red
    "error_700": "#C62828",       # Deep error
    
    "info_50": "#E1F5FE",
    "info_100": "#B3E5FC",
    "info_500": "#03A9F4",        # Information blue
    "info_700": "#0277BD",        # Deep info
    
    # Data-Specific Colors
    "sentiment_positive": "#4CAF50",   # Green for positive
    "sentiment_negative": "#F44336",   # Red for negative
    "sentiment_neutral": "#757575",    # Grey for neutral
    
    "quant_primary": "#9C27B0",        # Purple for quant
    "quant_secondary": "#BA68C8",       # Light purple
    
    "consensus_primary": "#FF9800",      # Amber for consensus
    "consensus_secondary": "#FFB74D",    # Light amber
    
    "regime_high_vol": "#F44336",       # Red for high volatility
    "regime_low_vol": "#4CAF50",        # Green for low volatility
    "regime_neutral": "#757575",        # Grey for neutral
    
    # Surface Colors
    "surface": "#FFFFFF",                # Pure white for main content
    "background": "#FAFAFA",            # Main background
    "card": "#FFFFFF",                  # Card backgrounds
    "sidebar": "#FAFAFA",               # Sidebar background
    
    # Border Colors
    "border_light": "#E0E0E0",         # Subtle borders
    "border_medium": "#BDBDBD",         # Standard borders
    "border_dark": "#757575",           # Emphasis borders
}

# ========================
# TYPOGRAPHY SYSTEM
# ========================
TYPOGRAPHY = {
    # Font Family (system fonts for performance)
    "font_family": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
    
    # Size Scale (8pt system for consistency)
    "text_xs": "10px",      # Small captions, labels
    "text_sm": "12px",      # Secondary text, metadata
    "text_base": "14px",     # Body text, standard content
    "text_lg": "16px",      # Large body, important content
    "text_xl": "18px",      # Small headings, section titles
    "text_2xl": "20px",     # Section headings
    "text_3xl": "24px",     # Page headings
    "text_4xl": "30px",     # Large page headings
    "text_5xl": "36px",     # Hero headings
    
    # Font Weights
    "weight_light": "300",
    "weight_normal": "400", 
    "weight_medium": "500",
    "weight_semibold": "600",
    "weight_bold": "700",
    "weight_black": "900",
    
    # Line Heights
    "leading_tight": "1.25",      # Headings
    "leading_normal": "1.5",       # Body text
    "leading_relaxed": "1.75",     # Large text
    "leading_loose": "2.0",        # Special cases
    
    # Letter Spacing
    "tracking_tight": "-0.025em",
    "tracking_normal": "0",
    "tracking_wide": "0.025em",
    "tracking_wider": "0.05em",
}

# ========================
# SPACING SYSTEM (8pt Grid)
# ========================
SPACING = {
    "0": "0px",
    "1": "4px",      # xs
    "2": "8px",      # sm  
    "3": "12px",     # md
    "4": "16px",     # lg
    "5": "20px",     # xl
    "6": "24px",     # 2xl
    "7": "28px",     # 3xl
    "8": "32px",     # 4xl
    "10": "40px",    # 5xl
    "12": "48px",    # 6xl
    "16": "64px",    # 7xl
    "20": "80px",    # 8xl
}

# ========================
# LAYOUT SYSTEM
# ========================
LAYOUT = {
    # Border Radius
    "radius_none": "0",
    "radius_sm": "4px",
    "radius_md": "8px",
    "radius_lg": "12px",
    "radius_xl": "16px",
    "radius_2xl": "20px",
    "radius_full": "9999px",
    
    # Shadows
    "shadow_sm": "0 1px 2px 0 rgba(0, 0, 0, 0.05)",
    "shadow_md": "0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)",
    "shadow_lg": "0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05)",
    "shadow_xl": "0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)",
    
    # Component Sizes
    "sidebar_width": "280px",
    "container_max": "1200px",
    "card_padding": "24px",
    "button_height_sm": "32px",
    "button_height_md": "40px", 
    "button_height_lg": "48px",
    "input_height_sm": "32px",
    "input_height_md": "40px",
    "input_height_lg": "48px",
}

# ========================
# COMPONENT STYLES
# ========================
COMPONENTS = {
    # Card Styles
    "card_elevated": {
        "background": COLORS["card"],
        "border_radius": LAYOUT["radius_lg"],
        "padding": LAYOUT["card_padding"],
        "box_shadow": LAYOUT["shadow_md"],
        "border": f"1px solid {COLORS['border_light']}",
        "margin_bottom": SPACING["4"],
    },
    
    "card_flat": {
        "background": COLORS["card"],
        "border_radius": LAYOUT["radius_md"],
        "padding": LAYOUT["card_padding"],
        "border": f"1px solid {COLORS['border_light']}",
        "margin_bottom": SPACING["4"],
    },
    
    # Button Styles
    "button_primary": {
        "background": COLORS["primary_800"],
        "color": COLORS["surface"],
        "border_radius": LAYOUT["radius_md"],
        "padding": f"{SPACING['3']} {SPACING['6']}",
        "font_weight": TYPOGRAPHY["weight_medium"],
        "font_size": TYPOGRAPHY["text_base"],
        "border": "none",
        "cursor": "pointer",
        "transition": "all 0.2s ease",
    },
    
    "button_secondary": {
        "background": "transparent",
        "color": COLORS["primary_800"],
        "border_radius": LAYOUT["radius_md"],
        "padding": f"{SPACING['3']} {SPACING['6']}",
        "font_weight": TYPOGRAPHY["weight_medium"],
        "font_size": TYPOGRAPHY["text_base"],
        "border": f"1px solid {COLORS['primary_800']}",
        "cursor": "pointer",
        "transition": "all 0.2s ease",
    },
    
    "button_ghost": {
        "background": "transparent",
        "color": COLORS["neutral_700"],
        "border_radius": LAYOUT["radius_md"],
        "padding": f"{SPACING['3']} {SPACING['6']}",
        "font_weight": TYPOGRAPHY["weight_normal"],
        "font_size": TYPOGRAPHY["text_base"],
        "border": "none",
        "cursor": "pointer",
        "transition": "all 0.2s ease",
    },
    
    # Input Styles
    "input_default": {
        "background": COLORS["surface"],
        "border_radius": LAYOUT["radius_md"],
        "padding": f"{SPACING['3']} {SPACING['4']}",
        "border": f"1px solid {COLORS['border_light']}",
        "font_size": TYPOGRAPHY["text_base"],
        "transition": "border-color 0.2s ease",
    },
    
    # Metric Styles
    "metric_primary": {
        "background": COLORS["surface"],
        "border_radius": LAYOUT["radius_lg"],
        "padding": LAYOUT["card_padding"],
        "border": f"1px solid {COLORS['border_light']}",
        "box_shadow": LAYOUT["shadow_sm"],
        "text_align": "center",
    },
}

# ========================
# CHART THEME
# ========================
CHART_THEME = {
    "layout": {
        "font": {
            "family": TYPOGRAPHY["font_family"],
            "size": TYPOGRAPHY["text_sm"],
            "color": COLORS["neutral_700"]
        },
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": COLORS["background"],
        "margin": {"t": 40, "r": 20, "b": 40, "l": 20},
        "colorway": [
            COLORS["primary_800"],
            COLORS["success_500"], 
            COLORS["warning_500"],
            COLORS["quant_primary"],
            COLORS["consensus_primary"]
        ],
        "showlegend": True,
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1
        },
        "hovermode": "x unified"
    },
    "xaxis": {
        "gridcolor": COLORS["border_light"],
        "zerolinecolor": COLORS["border_light"],
        "tickfont": {"color": COLORS["neutral_600"], "size": TYPOGRAPHY["text_xs"]},
        "titlefont": {"color": COLORS["neutral_700"], "size": TYPOGRAPHY["text_sm"]},
        "showgrid": True,
        "gridwidth": 1,
        "gridcolor": COLORS["neutral_200"],
    },
    "yaxis": {
        "gridcolor": COLORS["border_light"],
        "zerolinecolor": COLORS["border_light"], 
        "tickfont": {"color": COLORS["neutral_600"], "size": TYPOGRAPHY["text_xs"]},
        "titlefont": {"color": COLORS["neutral_700"], "size": TYPOGRAPHY["text_sm"]},
        "showgrid": True,
        "gridwidth": 1,
        "gridcolor": COLORS["neutral_200"],
    }
}

# ========================
# CSS INJECTION
# ========================

def inject_theme_css():
    """Inject custom CSS for consistent theming across Streamlit components"""
    css = f"""
    <style>
    /* CSS Custom Properties */
    :root {{
        --color-primary: {COLORS['primary_800']};
        --color-surface: {COLORS['surface']};
        --color-background: {COLORS['background']};
        --color-text-primary: {COLORS['neutral_900']};
        --color-text-secondary: {COLORS['neutral_600']};
        --color-border: {COLORS['border_light']};
        --spacing-unit: {SPACING['2']};
        --border-radius: {LAYOUT['radius_md']};
        --shadow-sm: {LAYOUT['shadow_sm']};
        --shadow-md: {LAYOUT['shadow_md']};
        --font-family: {TYPOGRAPHY['font_family']};
    }}
    
    /* Global Styles */
    .stApp {{
        background-color: {COLORS['background']};
        font-family: {TYPOGRAPHY['font_family']};
    }}
    
    /* Sidebar Styling */
    .css-1d391kg {{
        background-color: {COLORS['sidebar']};
        border-right: 1px solid {COLORS['border_light']};
    }}
    
    .css-1d391kg .css-17eq0hr {{
        background-color: transparent;
    }}
    
    /* Main Content Area */
    .main .block-container {{
        padding-top: {SPACING['6']};
        padding-bottom: {SPACING['6']};
        max-width: {LAYOUT['container_max']};
    }}
    
    /* Headers */
    h1, h2, h3, h4, h5, h6 {{
        font-family: {TYPOGRAPHY['font_family']};
        font-weight: {TYPOGRAPHY['weight_semibold']};
        color: {COLORS['neutral_900']};
        margin-top: {SPACING['6']};
        margin-bottom: {SPACING['4']};
    }}
    
    h1 {{ font-size: {TYPOGRAPHY['text_4xl']}; }}
    h2 {{ font-size: {TYPOGRAPHY['text_3xl']}; }}
    h3 {{ font-size: {TYPOGRAPHY['text_2xl']}; }}
    h4 {{ font-size: {TYPOGRAPHY['text_xl']}; }}
    
    /* Metrics */
    div[data-testid="metric-container"] {{
        background-color: {COLORS['surface']};
        border: 1px solid {COLORS['border_light']};
        border-radius: {LAYOUT['radius_lg']};
        padding: {SPACING['4']};
        box-shadow: {LAYOUT['shadow_sm']};
    }}
    
    /* Cards and Containers */
    .streamlit-expanderHeader {{
        background-color: {COLORS['surface']};
        border-radius: {LAYOUT['radius_md']};
        border: 1px solid {COLORS['border_light']};
    }}
    
    /* Buttons */
    .stButton > button {{
        background-color: {COLORS['primary_800']};
        color: {COLORS['surface']};
        border-radius: {LAYOUT['radius_md']};
        border: none;
        font-weight: {TYPOGRAPHY['weight_medium']};
        transition: all 0.2s ease;
    }}
    
    .stButton > button:hover {{
        background-color: {COLORS['primary_900']};
        box-shadow: {LAYOUT['shadow_md']};
    }}
    
    /* DataFrames */
    .dataframe {{
        border-radius: {LAYOUT['radius_md']};
        overflow: hidden;
        box-shadow: {LAYOUT['shadow_sm']};
    }}
    
    /* Sidebar Elements */
    .css-1d391kg h1 {{
        color: {COLORS['primary_800']};
        font-size: {TYPOGRAPHY['text_2xl']};
    }}
    
    .css-1d391kg .stSelectbox > div > div {{
        background-color: {COLORS['surface']};
        border-radius: {LAYOUT['radius_md']};
        border: 1px solid {COLORS['border_light']};
    }}
    
    .css-1d391kg .stSlider > div > div {{
        background-color: {COLORS['surface']};
    }}
    
    /* Remove Streamlit Branding */
    .stDeployButton {{
        display: none;
    }}
    
    /* Custom Scrollbar */
    ::-webkit-scrollbar {{
        width: 8px;
    }}
    
    ::-webkit-scrollbar-track {{
        background: {COLORS['neutral_100']};
    }}
    
    ::-webkit-scrollbar-thumb {{
        background: {COLORS['neutral_400']};
        border-radius: {LAYOUT['radius_full']};
    }}
    
    ::-webkit-scrollbar-thumb:hover {{
        background: {COLORS['neutral_500']};
    }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

def apply_theme():
    """Apply the enhanced theme to the current Streamlit app"""
    inject_theme_css()
    
    # Set page config with enhanced styling
    st.set_page_config(
        layout="wide",
        page_title="Alpha Engine Dashboard",
        page_icon="🚀",
        initial_sidebar_state="expanded"
    )

# ========================
# UTILITY FUNCTIONS
# ========================

def get_color_for_direction(direction: str) -> str:
    """Get color based on signal direction"""
    direction = str(direction).strip().lower()
    if direction in ('up', 'long', 'buy', '1', '+1'):
        return COLORS['sentiment_positive']
    elif direction in ('down', 'short', 'sell', '-1'):
        return COLORS['sentiment_negative']
    else:
        return COLORS['sentiment_neutral']

def get_color_for_strategy(strategy_type: str) -> str:
    """Get color based on strategy type"""
    strategy_type = str(strategy_type).strip().lower()
    if 'sentiment' in strategy_type:
        return COLORS['sentiment_positive']
    elif 'quant' in strategy_type:
        return COLORS['quant_primary']
    elif 'consensus' in strategy_type:
        return COLORS['consensus_primary']
    else:
        return COLORS['primary_800']

def format_metric_value(value: Any, precision: int = 2) -> str:
    """Format metric values with consistent styling"""
    try:
        if isinstance(value, (int, float)):
            if abs(value) >= 1000000:
                return f"{value/1000000:.{precision}f}M"
            elif abs(value) >= 1000:
                return f"{value/1000:.{precision}f}K"
            else:
                return f"{value:.{precision}f}"
        return str(value)
    except:
        return "—"
